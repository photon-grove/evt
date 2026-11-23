// Runs the conformance suites against PostgreSQL at DATABASE_URL, by default the database
// infra/local-postgres creates for Go's suite, and only ever a local server. Each run creates its
// own event logs, with table names unique to the run, and drops them at the end, so it can share
// the database with the Go and Rust suites. Each step needs the one before it, so the steps await
// in loops on purpose.
/* oxlint-disable no-await-in-loop */
import process from 'node:process'
import {Client, Pool} from 'pg'

import {
  CommitTooLargeError,
  POSTGRES_VIEWS_FEATURE,
  type ProjectedView,
  ProjectorError,
  ProjectorOperationError,
  type SerializedEvent,
  StreamHeadsNotMigratedError,
  TransientError,
  UnsupportedFeatureError,
  ViewKeyTooLongError,
  type ViewOperation,
  ViewsNotConfiguredError,
  type ViewRow,
  emptyMetadata,
  encodeMetadata,
  deleteView,
  eventId,
  putView,
} from '@photon-grove/evt'
import {Journal, JournalRepository, rebuildJournalWithLock} from '@photon-grove/evt/journal'
import {JOURNAL_MARKER} from '@photon-grove/evt/testing'
import {
  JournalListener,
  type PgClient,
  PgRepository,
  type ViewWriter,
  type Wakeup,
} from '@photon-grove/evt/postgres'

import {
  type ControlFixtures,
  type HeadFixtures,
  conformance,
  markerConformance,
} from './conformance.js'
import {postgresControls} from './controls.js'
import {TestClock, compactionConformance} from './compaction-conformance.js'
import {Checks} from './harness.js'
import {
  type JournalRebuildTarget,
  journalConformance,
  journalRebuildConformance,
  journalStoreCases,
} from './journal-conformance.js'
import {postgresViewDdl, postgresViewWriter} from './pgviews.js'
import {committedViewConformance, viewRepositoryConformance} from './view-conformance.js'

// The local-only placeholder credentials infra/local-postgres creates.
const url = process.env['DATABASE_URL'] || 'postgres://evt:evt@localhost:5432/evt_local'
const host = URL.canParse(url) ? new URL(url).hostname : ''
if (!['localhost', '127.0.0.1', '[::1]'].includes(host) && !host.endsWith('.localhost')) {
  throw new Error(`DATABASE_URL must name a local PostgreSQL server: ${url}`)
}

const pool = new Pool({connectionString: url, max: 8})
const run = Date.now().toString(36)
const logs: string[] = []
let logCount = 0

const checks = new Checks()
try {
  // The suite's store is initialized up front, as a deployment would.
  const repo = await freshLog()
  await repo.initialize()
  await conformance(checks, repo, headFixtures(repo))

  // The marker cases declare store-wide, so they get a log of their own.
  const marked = await freshLog()
  await markerConformance(checks, marked, controls(marked))

  // A client without connect() is one connection, which the repository runs one operation at a
  // time on, as it would PGlite.
  const connection = await pool.connect()
  try {
    const single: PgClient = {query: (text, values) => connection.query(text, values)}
    const serial = await freshLog(single)
    await conformance(checks, serial, headFixtures(serial))
  } finally {
    connection.release()
  }

  await postgresCases()
  await viewsTableCases()
  await journalCases()
  await wakeupCases()
  await compactionCases()
} finally {
  for (const events of logs) {
    await pool.query(`DROP TABLE IF EXISTS ${tables(events).join(', ')}`)
  }
  await pool.end()
}

checks.report()

// A fresh event log, under table names unique to this run, with its schema in place, and a view
// table of its own that its repository writes through the compat drivers' view writer.
async function freshLog(
  client: PgClient = pool,
  viewWriter?: (views: string) => ViewWriter,
): Promise<PgRepository> {
  const events = `evt_ts_${run}_${++logCount}`
  logs.push(events)
  const views = `${events}_views`
  await pool.query(postgresViewDdl(views))
  const repo = new PgRepository(client, {
    eventsTable: events,
    snapshotsTable: `${events}_snaps`,
    viewWriter: (viewWriter ?? postgresViewWriter)(views),
  })
  await repo.ensureSchema()

  return repo
}

// Every table a log's schema creates.
function tables(events: string): string[] {
  const derived = [
    'command_outcomes',
    'features',
    'rebuild_lock',
    'rebuild_generation',
    'stream_heads',
    'views',
  ]

  return [
    events,
    `${events}_snaps`,
    `${events}_views`,
    ...derived.map((suffix) => `${events}__evt_${suffix}`),
  ]
}

// What PostgreSQL alone does: initialize a fresh store on the first write, keep a cached writer
// writing across a declaration it supports, and keep two logs in one database apart.
async function postgresCases(): Promise<void> {
  await checks.case('postgres: ensureSchema is idempotent, across concurrent calls', async () => {
    const repo = await freshLog()
    const others = [0, 1, 2].map(() => new PgRepository(pool, repoTables(repo)))
    await Promise.all([repo, ...others].map((each) => each.ensureSchema()))
    await repo.commit({entityId: 'a', events: [ledgerEvent('a', 1)]})
    await repo.ensureSchema()
    ensure((await repo.events('a')).length === 1, 'the schema kept its rows')
  })

  await checks.case("postgres: a writer's first write declares stream-heads", async () => {
    const repo = await freshLog()
    await repo.commit({entityId: 'a', events: [ledgerEvent('a', 1)]})
    const manifest = await controls(repo).manifest()
    ensure(manifest?.version === 1n, 'the first write declares version 1')
    ensure(manifest.features.get('stream-heads') === 1, 'it declares stream-heads')
  })

  await checks.case('postgres: initialize declares once, across concurrent calls', async () => {
    const repo = await freshLog()
    const repos = [0, 1, 2].map(() => new PgRepository(pool, repoTables(repo)))
    const declared = await Promise.all(repos.map((each) => each.initialize()))
    ensure(declared.filter(Boolean).length === 1, `one call declares: got ${declared}`)
    ensure(!(await repo.initialize()), 'initializing again declares nothing')
    ensure((await controls(repo).manifest())?.version === 1n, 'the version bumps once')
  })

  await checks.case('postgres: a store holding data without stream-heads is refused', async () => {
    const repo = await freshLog()
    await headFixtures(repo).appendBypassingHead(ledgerEvent('a', 1))
    await refuses(repo.initialize(), StreamHeadsNotMigratedError, 'initializing')
    await refuses(
      repo.commit({entityId: 'b', events: [ledgerEvent('b', 1)]}),
      StreamHeadsNotMigratedError,
      'a commit',
    )
    ensure((await controls(repo).manifest()) === null, 'nothing was declared')
  })

  await checks.case('postgres: a cached writer validates a declaration as it commits', async () => {
    const repo = await freshLog()
    await repo.commit({entityId: 'a', events: [ledgerEvent('a', 1)]})

    // The commit reads the manifest under the shared lock, so a supported declaration needs no
    // retry, and an unsupported one refuses the next commit.
    await controls(repo).declare((draft) => draft.retention.push('session'))
    await repo.commit({entityId: 'a', events: [ledgerEvent('a', 2)]})
    await controls(repo).declare((draft) => draft.features.set('journal', 1))
    await refuses(
      repo.commit({entityId: 'a', events: [ledgerEvent('a', 3)]}),
      UnsupportedFeatureError,
      'a commit after an unsupported declaration',
    )
    ensure((await repo.events('a')).length === 2, 'the refused commit wrote nothing')
  })

  await checks.case('postgres: two logs in one database keep their engine rows apart', async () => {
    const first = await freshLog()
    const second = await freshLog()
    await first.commit({entityId: 'a', events: [ledgerEvent('a', 1)]})
    await controls(first).declare((draft) => draft.features.set('journal', 1))
    await controls(first).setRebuild({generation: 1n, lock: 'held'})

    // The same entity in the other log has its own head, marker and rebuild fence.
    await second.commit({entityId: 'a', events: [ledgerEvent('a', 1)]})
    await second.commit({entityId: 'a', events: [ledgerEvent('a', 2)]})
    ensure((await second.events('a')).length === 2, 'the other log took both commits')
  })

  await checks.case('postgres: a commit runs at READ COMMITTED whatever the default', async () => {
    const repo = await freshLog()
    const strict = new Pool({
      connectionString: url,
      max: 2,
      options: '-c default_transaction_isolation=serializable',
    })
    try {
      const seen: string[] = []
      const client: PgClient = {
        query: (text, values) => strict.query(text, values),
        async connect() {
          const connection = await strict.connect()
          return {
            async query(text: string, values?: unknown[]) {
              const result = await connection.query(text, values)
              if (text.startsWith('SELECT pg_advisory_xact_lock(1702261760')) {
                const level = await connection.query('SHOW transaction_isolation')
                seen.push(String(level.rows[0]?.transaction_isolation))
              }
              return result
            },
            release: (error?: Error) => connection.release(error),
          }
        },
      }
      await new PgRepository(client, repoTables(repo)).commit({
        entityId: 'a',
        events: [ledgerEvent('a', 1)],
      })
      ensure(seen.join() === 'read committed', `the commit's isolation: got ${seen.join()}`)
    } finally {
      await strict.end()
    }
  })

  await checks.case(
    "postgres: a view statement's database errors are its operation's",
    async () => {
      const repo = await freshLog()
      const views = `${repo.tables.events}_views`
      // A unique payload is another constraint than the key, and a CHECK rejects one payload.
      await pool.query(`ALTER TABLE ${views} ADD CONSTRAINT ${views}_payload_key UNIQUE (payload)`)
      await pool.query(`ALTER TABLE ${views} ADD CONSTRAINT ${views}_sane CHECK (payload <> '{}')`)
      await repo.commit({
        entityId: 'a',
        events: [ledgerEvent('a', 1)],
        views: [projected(putView('k', 'a', 'a', 'ledger', '{"n":1}', {condition: 'absent'}))],
      })

      const claim = (entityId: string, payload: string) =>
        repo.commit({
          entityId,
          events: [ledgerEvent(entityId, 1)],
          views: [
            projected(putView('k', entityId, entityId, 'ledger', payload, {condition: 'absent'})),
          ],
        })
      await reason(claim('b', '{"n":1}'), '23505', 'a unique payload of another row')
      await reason(claim('b', '{}'), '23514', 'a CHECK violation')
      await reason(
        repo.commit({
          entityId: 'b',
          events: [ledgerEvent('b', 1)],
          views: [projected(putView('k', 'a', 'b', 'ledger', '{"n":2}', {condition: 'absent'}))],
        }),
        'ConditionalCheckFailed',
        'a claimed key',
      )
      ensure((await repo.events('b')).length === 0, 'the refused commits wrote no event')

      const {rows} = await pool.query(`SELECT sk FROM ${views}`)
      ensure(rows.length === 1, `the refused commits wrote no row: got ${rows.length}`)
    },
  )

  await checks.case(
    'postgres: an integrity violation at COMMIT is the projector failure',
    async () => {
      const repo = await freshLog()
      const views = `${repo.tables.events}_views`
      await pool.query(
        `ALTER TABLE ${views} ADD CONSTRAINT ${views}_payload_key UNIQUE (payload) ` +
          'DEFERRABLE INITIALLY DEFERRED',
      )
      const twice = [
        projected(putView('k', 'one', 'a', 'ledger', '{"n":1}')),
        projected(putView('k', 'two', 'a', 'ledger', '{"n":1}'), 1),
      ]
      try {
        await repo.commit({entityId: 'a', events: [ledgerEvent('a', 1)], views: twice})
      } catch (error) {
        ensure(error instanceof ProjectorError, `got ${String(error)}`)
        ensure(error.projector === '(at commit)', `the projector: got ${error.projector}`)
        ensure((await repo.events('a')).length === 0, 'the commit rolled back')

        return
      }
      throw new Error('the commit resolved')
    },
  )

  await checks.case(
    'postgres: RETURNING statements serve a driver that reports no count',
    async () => {
      const repo = await freshLog(countless(), returning)
      const put = (entityId: string, owner: string) =>
        repo.commit({
          entityId,
          events: [ledgerEvent(entityId, 1)],
          views: [projected(putView('k', 'one', owner, 'ledger', `{"by":"${owner}"}`))],
        })
      await put('a', 'a')
      await reason(put('b', 'b'), 'ConditionalCheckFailed', "an overwrite of another owner's row")

      const release = (sequence: number, op: ViewOperation) =>
        repo.commit({entityId: 'a', events: [ledgerEvent('a', sequence)], views: [projected(op)]})
      await release(2, deleteView('k', 'one', 'a', {condition: 'present'}))
      const absent = deleteView('k', 'one', 'a', {condition: 'present'})
      await reason(release(3, absent), 'ConditionalCheckFailed', 'a release of an absent row')
      await release(3, deleteView('k', 'one', 'a'))
      ensure((await repo.events('a')).length === 3, 'the held conditions committed')
    },
  )
}

// A fresh event log whose commits write their views to its views table.
async function tableLog(clock?: () => number): Promise<PgRepository> {
  const events = `evt_ts_${run}_${++logCount}`
  logs.push(events)
  const repo = new PgRepository(pool, {
    eventsTable: events,
    snapshotsTable: `${events}_snaps`,
    viewsTable: true,
    ...(clock && {clock}),
  })
  await repo.ensureSchema()

  return repo
}

// The evt-owned views table (ADR 0008), as Rust's postgres_view_repository_conforms and
// postgres_views_table_conforms run it: the view repository cases, then what is PostgreSQL's own;
// the conformance suite with the views table as the commit's view target; the committed rows
// through the view repository; the budget; and the postgres-views marker feature.
async function viewsTableCases(): Promise<void> {
  const noise = noiseSource()
  const reader = (await tableLog()).viewRepository()
  await viewRepositoryConformance(checks, 'postgres', reader)

  // A repository's own view repository reads the views table its commits write.
  const writer = await tableLog()
  await committedViewConformance(checks, 'postgres', writer, writer.viewRepository())

  await checks.case('postgres views: payloads are exact and keys order by bytes', async () => {
    // The payload is text, not jsonb: whitespace and key order survive.
    const exact = '{ "b": 1,   "a": [ 2 ] }'
    await reader.putView(viewRow('exact', 'VIEW', exact))
    const stored = await reader.getView('exact')
    ensure(decode(stored?.payload) === exact, `the payload bytes: got ${decode(stored?.payload)}`)

    // "Z" (0x5A) < "b" (0x62) < "é" (0xC3 0xA9), as on DynamoDB, whatever the database collation.
    for (const sk of ['é', 'b', 'Z']) {
      await reader.putView(viewRow('ordered', sk))
    }
    const order = (await reader.listViewsByPk('ordered')).map((row) => row.sk)
    ensure(order.join() === 'Z,b,é', `bytewise order: got ${order.join()}`)
  })

  await checks.case('postgres views: a key the btree indexes cannot hold is refused', async () => {
    const long = {...viewRow('p'.repeat(2048), 's'.repeat(1000)), entityType: 'kind'}
    await refusesWith(reader.putView(long), 'invalidArgument', '2600', 'a key over the bound')

    // At the bound, an incompressible key fits both indexes.
    const boundary = {...viewRow(noise(1598), noise(998)), entityType: noise(4)}
    await reader.putView(boundary)
    ensure((await reader.getViewAt(boundary.pk, boundary.sk)) !== null, 'the key at the bound')
  })

  await checks.case("postgres views: a batch keeps a repeated key's last row", async () => {
    await reader.putViews([
      viewRow('batch', 'b', '{"n":1}'),
      viewRow('batch', 'a', '{"n":2}'),
      viewRow('batch', 'b', '{"n":3}'),
    ])
    const rows = (await reader.listViewsByPk('batch')).map(
      (row) => `${row.sk}=${decode(row.payload)}`,
    )
    ensure(rows.join() === 'a={"n":2},b={"n":3}', `the batch: got ${rows.join()}`)
  })

  await checks.case('postgres views: an unbounded page holds 1,000 rows and a cursor', async () => {
    const many = Array.from({length: 1001}, (_, i) => ({
      ...viewRow('many', String(i).padStart(4, '0')),
      entityType: 'many',
    }))
    await reader.putViews(many)
    const first = await reader.listViewsByPkPaged('many')
    ensure(first.views.length === 1000, `the first page: got ${first.views.length}`)
    ensure(first.nextCursor !== null, 'a full unbounded page has a cursor')
    ensure((await reader.listViewsByPk('many')).length === 1001, 'the whole partition')
    ensure((await reader.listViewsByEntityType('many')).length === 1001, 'the whole entity type')
  })

  await checks.case('postgres views: purgeExpired deletes only the expired rows', async () => {
    await reader.putViews([
      {...viewRow('ttl', 'old'), ttl: 1000},
      {...viewRow('ttl', 'new'), ttl: 4_102_444_800},
      viewRow('ttl', 'none'),
    ])
    ensure((await reader.getViewAt('ttl', 'old')) !== null, 'an expired row reads until a purge')
    const purged = await reader.purgeExpired(2000)
    ensure(purged === 1, `purged: got ${purged}`)
    const left = (await reader.listViewsByPk('ttl')).map((row) => row.sk)
    ensure(left.join() === 'new,none', `left: got ${left.join()}`)
  })

  // The conformance suite, its commit-wide views case writing to the views table.
  const target = await tableLog()
  await target.initialize()
  await conformance(checks, target, headFixtures(target))

  await checks.case('postgres views table: committed rows read back as stored', async () => {
    const repo = await tableLog()
    const views = repo.viewRepository()
    // A row written outside a commit, with a ttl, which the owner's commit then replaces.
    await views.putView({...viewRow('acct', 'VIEW', '{"old":true}'), entityId: 'a', ttl: 99})
    await repo.commit({
      entityId: 'a',
      events: [ledgerEvent('a', 1)],
      views: [
        projected(putView('acct', 'VIEW', 'a', 'account', '{ "n": 1 }')),
        projected(putView('acct', 'gone', 'a', 'account', '{}'), 1),
      ],
    })
    await repo.commit({
      entityId: 'a',
      events: [ledgerEvent('a', 2)],
      views: [projected(deleteView('acct', 'gone', 'a', {condition: 'present'}))],
    })

    const rows = await views.listViewsByPk('acct')
    const shown = rows.map((row) => [
      row.sk,
      row.entityId,
      row.entityType,
      decode(row.payload),
      row.ttl,
    ])
    ensure(
      JSON.stringify(shown) === JSON.stringify([['VIEW', 'a', 'account', '{ "n": 1 }', 0]]),
      `the rows: got ${JSON.stringify(shown)}`,
    )
    const listed = await views.listViewsByEntityType('account')
    ensure(listed.length === 1 && listed[0]!.pk === 'acct', 'listed by entity type')
  })

  await checks.case('postgres views table: each view counts as its DynamoDB item', async () => {
    const repo = await tableLog()
    const puts = (count: number) =>
      Array.from({length: count}, (_, n) =>
        projected(putView('k', String(n).padStart(3, '0'), 'a', 'ledger', '{}'), n),
      )
    // Two control checks, the head and the event leave room for 96 views.
    await refuses(
      repo.commit({entityId: 'a', events: [ledgerEvent('a', 1)], views: puts(97)}),
      CommitTooLargeError,
      '97 views',
    )
    const big = `"${'x'.repeat(400_000)}"`
    await refuses(
      repo.commit({
        entityId: 'a',
        events: [ledgerEvent('a', 1)],
        views: [projected(putView('k', 'big', 'a', 'ledger', big))],
      }),
      CommitTooLargeError,
      'a view over the item limit',
    )
    ensure((await repo.events('a')).length === 0, 'the refused commits wrote nothing')
    await repo.commit({entityId: 'a', events: [ledgerEvent('a', 1)], views: puts(96)})
    ensure((await repo.viewRepository().listViewsByPk('k')).length === 96, 'the rows of 96 views')
  })

  await checks.case(
    'postgres views table: a key over the index bound is refused before any write',
    async () => {
      const repo = await tableLog()
      const long = putView('p'.repeat(2048), 's'.repeat(549), 'a', 'abcd', '{}')
      await refuses(
        repo.commit({entityId: 'a', events: [ledgerEvent('a', 1)], views: [projected(long)]}),
        ViewKeyTooLongError,
        'a key a byte over the bound',
      )
      ensure((await repo.events('a')).length === 0, 'the refused commit wrote nothing')
      const boundary = putView(noise(1598), noise(998), 'a', noise(4), '{}')
      await repo.commit({
        entityId: 'a',
        events: [ledgerEvent('a', 1)],
        views: [projected(boundary)],
      })
    },
  )

  await checks.case(
    'postgres views table: the first commit declares postgres-views, and other writers refuse it',
    async () => {
      const repo = await tableLog()
      await repo.commit({entityId: 'a', events: [ledgerEvent('a', 1)]})
      const manifest = await controls(repo).manifest()
      ensure(manifest?.features.get(POSTGRES_VIEWS_FEATURE) === 1, 'the feature is declared')

      const {events, snapshots} = repo.tables
      const others = [
        new PgRepository(pool, {eventsTable: events, snapshotsTable: snapshots}),
        new PgRepository(pool, {
          eventsTable: events,
          snapshotsTable: snapshots,
          viewWriter: postgresViewWriter(`${events}_views`),
        }),
      ]
      for (const other of others) {
        await refuses(
          other.commit({entityId: 'b', events: [ledgerEvent('b', 1)]}),
          UnsupportedFeatureError,
          'a commit by a writer without the views table',
        )
        await refuses(
          other.putSnapshot({
            entityType: 'ledger',
            entityId: 'b',
            sequence: 0n,
            eventSequence: 0n,
            payload: new Uint8Array(),
          }),
          UnsupportedFeatureError,
          'a snapshot write by a writer without the views table',
        )
        await refuses(
          other.compactBelow('a', 1n),
          UnsupportedFeatureError,
          'a compaction by a writer without the views table',
        )
      }
      ensure((await repo.events('b')).length === 0, 'the refused writers wrote nothing')

      // Initialization still works on the declared store, as a deploy step runs it.
      ensure(!(await repo.initialize()), 'already initialized')
      ensure(!(await others[0]!.initialize()), 'already initialized, for any writer')
    },
  )

  await checks.case(
    'postgres views table: a store that lost the declaration is declared again on retry',
    async () => {
      const repo = await tableLog()
      await repo.commit({entityId: 'a', events: [ledgerEvent('a', 1)]})
      await controls(repo).declare((draft) => draft.features.delete(POSTGRES_VIEWS_FEATURE))
      await refuses(
        repo.commit({entityId: 'a', events: [ledgerEvent('a', 2)]}),
        TransientError,
        'a commit after the declaration was lost',
      )
      await repo.commit({entityId: 'a', events: [ledgerEvent('a', 2)]})
      const manifest = await controls(repo).manifest()
      ensure(manifest?.features.get(POSTGRES_VIEWS_FEATURE) === 1, 'declared again')
      ensure((await repo.events('a')).length === 2, 'the retry committed')
    },
  )
}

// The journal (ADR 0005) on the views table (ADR 0008), as Rust's postgres_journal_conforms runs
// it: the shared journal cases over a log of their own, the fresh-store cases, and a repository
// without the views table, which holds no journal.
async function journalCases(): Promise<void> {
  const repo = await tableLog()
  const reader = repo.viewRepository()
  const journalTarget: JournalRebuildTarget = {
    repo,
    manifest: async () => controls(repo).manifest(),
    views: {
      getViewAt: (pk, sk) => reader.getViewAt(pk, sk),
      putView: (row) => reader.putView(row),
      deleteView: (pk, sk) => reader.deleteView(pk, sk),
      listViewsByPk: (pk) => reader.listViewsByPk(pk),
    },
    handle: () => new PgRepository(pool, {...repoTables(repo), viewsTable: true}),
  }
  await journalConformance(checks, journalTarget)
  // The rebuild cases take the store-wide rebuild lock: after the others, on the same store.
  await journalRebuildConformance(checks, journalTarget)

  await checks.case(
    "postgres journal: a rebuild writes the journal's tables through any handle, which still refuses its own commits",
    async () => {
      const log = await tableLog()
      const journal = new Journal(['ledger'], (event) => [`ledger:${event.entityId}`])
      await new JournalRepository(log, journal).commit({
        entityId: 'a',
        events: [ledgerEvent('a', 1), ledgerEvent('a', 2)],
      })
      const views = log.viewRepository()
      const rowPk = 'journal#ledger:a'
      for (const row of await views.listViewsByPk(rowPk)) {
        await views.deleteView(row.pk, row.sk)
      }

      // A handle no journal wraps rebuilds the journal's rows on the store that declares it.
      const plain = new PgRepository(pool, {...repoTables(log), viewsTable: true})
      await rebuildJournalWithLock(plain, await plain.events('a'), 60_000)
      ensure((await views.listViewsByPk(rowPk)).length === 2, 'the rows are restored')
      ensure((await plain.rebuildState()).lock === null, 'the lock is released')

      // Its own commits are still a non-journal writer's, refused on a journal store.
      await refuses(
        plain.commit({entityId: 'b', events: [ledgerEvent('b', 1)]}),
        UnsupportedFeatureError,
        'a plain commit',
      )
      ensure((await plain.events('b')).length === 0, 'nothing was written')
    },
  )
  await journalStoreCases(checks, async () => {
    const fresh = await tableLog()

    return {repo: fresh, manifest: async () => controls(fresh).manifest()}
  })

  await checks.case(
    'postgres journal: compaction rows are read from the views table, and compaction declared once',
    async () => {
      const log = await tableLog()
      const views = log.viewRepository()
      ensure((await log.readCompactionRow('s', 'LEASE')) === null, 'no row yet')
      const lease = {
        pk: 'journal-compaction#s',
        sk: 'LEASE',
        entityId: 'SYSTEM#JOURNAL',
        entityType: 'journal-compaction',
        payload: new TextEncoder().encode('{"owner":"o"}'),
      }
      await views.putView(lease)
      const read = await log.readCompactionRow('s', 'LEASE')
      ensure(decode(read?.payload) === '{"owner":"o"}', `the row: ${decode(read?.payload)}`)
      ensure((await log.readCompactionRow('t', 'LEASE')) === null, 'another scope has none')

      // Any handle checks and declares compaction, once, and refuses another version of it.
      const marker = log[JOURNAL_MARKER]
      await marker.checkCompactionMarker()
      await marker.declareCompaction()
      const declared = await controls(log).manifest()
      ensure(declared?.features.get('journal-compaction') === 1, 'declared')
      await marker.declareCompaction()
      await marker.checkCompactionMarker()
      ensure((await controls(log).manifest())?.version === declared.version, 'declared once')
      await controls(log).declare((draft) => draft.features.set('journal-compaction', 2))
      await refuses(marker.checkCompactionMarker(), UnsupportedFeatureError, 'another version')
    },
  )

  await checks.case(
    'postgres journal: a repository without the views table holds no journal',
    async () => {
      const plain = await freshLog()
      for (const [what, read] of [
        ['readCounter', () => plain.readCounter('s')],
        ['readCounters', () => plain.readCounters(['s'])],
        ['readLastPosition', () => plain.readLastPosition('s')],
        ['readRows', () => plain.readRows('s', 0n, 10)],
        ['readCompactionRow', () => plain.readCompactionRow('s', 'LEASE')],
      ] as const) {
        await refuses(read(), ViewsNotConfiguredError, what)
      }

      // A journaled commit through it is refused before anything is declared or written.
      const journaled = new JournalRepository(
        plain,
        new Journal(['ledger'], (event) => [`ledger:${event.entityId}`]),
      )
      await refusesWith(
        journaled.commit({entityId: 'a', events: [ledgerEvent('a', 1)]}),
        'expectedPayloadUnsupported',
        'expected-payload',
        'a journaled commit',
      )
      ensure((await controls(plain).manifest()) === null, 'nothing was declared')
      ensure((await plain.events('a')).length === 0, 'nothing was written')
    },
  )
}

// Journal compaction (ADR 0008, X4), as Rust's postgres_journal_conforms runs it: the shared
// compaction cases over a log of their own, with a clock the lease cases move, then a batch that
// finds a key absent and meets a row another transaction inserted there meanwhile.
async function compactionCases(): Promise<void> {
  const clock = new TestClock(Date.now())
  const repo = await tableLog(clock.now)
  await repo.initialize()
  const reader = repo.viewRepository()
  await compactionConformance(checks, {
    repo,
    clock,
    views: {
      getViewAt: (pk, sk) => reader.getViewAt(pk, sk),
      putView: (row) => reader.putView(row),
      deleteView: (pk, sk) => reader.deleteView(pk, sk),
    },
    manifest: async () => controls(repo).manifest(),
    declare: async (change) => {
      await controls(repo).declare(change)
      // The repository caches the manifest it validated: have it read the new one now, as its
      // next write would. A manifest it refuses stays refused.
      try {
        await repo.initialize()
      } catch (error) {
        if (!(error instanceof UnsupportedFeatureError)) {
          throw error
        }
      }
    },
  })

  await checks.case(
    'postgres journal compaction: an absent put that meets a row inserted meanwhile is refused',
    async () => {
      const log = await tableLog()
      const table = log.viewRepository().table

      // Another transaction inserts the lease row and holds it uncommitted.
      const holder = await pool.connect()
      let open = false
      let racing: Promise<unknown> = Promise.resolve()
      try {
        await holder.query('BEGIN')
        open = true
        const theirs = raceLease('theirs')
        await holder.query(
          `INSERT INTO ${table} (pk, sk, entity_id, entity_type, payload, ttl) ` +
            'VALUES ($1, $2, $3, $4, $5, 0)',
          [theirs.pk, theirs.sk, theirs.entityId, theirs.entityType, decode(theirs.payload)],
        )
        const batch = log.writeCompaction(null, [
          {kind: 'put', row: raceLease('ours'), expect: {kind: 'absent'}},
        ])
        racing = batch.catch(() => undefined)

        // The batch's insert waits on the holder's transaction.
        const deadline = Date.now() + 5000
        for (;;) {
          const {rows} = await pool.query(
            "SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE $1",
            [`INSERT INTO ${table} %`],
          )
          if (rows.length > 0) {
            break
          }
          ensure(Date.now() < deadline, 'the batch never waited on the other insert')
          await delay(10)
        }
        await holder.query('COMMIT')
        open = false

        const written = await batch
        ensure(json(written) === json({kind: 'refused', at: 0}), `refused: ${json(written)}`)
        const stored = await log.viewRepository().getViewAt('journal-compaction#race', 'LEASE')
        ensure(decode(stored?.payload).includes('theirs'), "the other transaction's row stands")
      } finally {
        // A failure must not leave the insert holding the key, or the batch waiting on it.
        if (open) {
          await holder.query('ROLLBACK').catch(() => undefined)
        }
        holder.release()
        await racing
      }
    },
  )
}

// The lease row of the `race` scope, with the payload `token` names.
function raceLease(token: string): ViewRow {
  return {
    pk: 'journal-compaction#race',
    sk: 'LEASE',
    entityId: 'SYSTEM#JOURNAL',
    entityType: 'journal-compaction',
    payload: new TextEncoder().encode(`{"scope":"race","token":"${token}"}`),
  }
}

function json(value: unknown): string {
  return JSON.stringify(value)
}

// Journal wake-ups on PostgreSQL (ADR 0008), as Rust's postgres_journal_wakeups runs them: a
// journaled commit through the views table notifies its scopes at COMMIT; a refused commit and
// `wakeups: false` notify nothing; a listener whose connection drops reconnects and says so; and
// neither steady traffic nor a backlog holds back the poll.
async function wakeupCases(): Promise<void> {
  await checks.case('postgres journal: wake-ups', async () => {
    const repo = await tableLog()
    const names = repoTables(repo)
    const journal = new Journal(['ledger'], (event) => [`ledger:${event.entityId}`])
    const journaled = new JournalRepository(repo, journal)
    const quiet = new JournalRepository(
      new PgRepository(pool, {...names, viewsTable: true, wakeups: false}),
      journal,
    )
    const application = `${names.eventsTable}-listener`
    const listener = new JournalListener({
      eventsTable: names.eventsTable,
      pollIntervalMs: 500,
      connect: async () => {
        const client = new Client({connectionString: url, application_name: application})
        await client.connect()

        return client
      },
    })
    try {
      ensure(listener.channel === `${names.eventsTable}__evt_journal`, 'the channel')
      ensureWakeup(await listener.next(), {kind: 'connected'}, 'the first call connects')

      // A journaled commit wakes its scope.
      await journaled.commit({entityId: 'a', events: [ledgerEvent('a', 1)]})
      ensureWakeup(await listener.next(), {kind: 'scope', scope: 'ledger:a'}, 'a commit wakes')

      // A refused commit and a repository with wake-ups off notify nothing.
      await refuses(
        journaled.commit({entityId: 'a', events: [ledgerEvent('a', 1)]}),
        Error,
        'a conflicting commit',
      )
      await quiet.commit({entityId: 'b', events: [ledgerEvent('b', 1)]})
      ensureWakeup(await listener.next(), {kind: 'poll'}, 'nothing woke before the poll')

      // The connection drops: the listener reconnects, says so, and hears later commits.
      await pool.query(
        'SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1',
        [application],
      )
      const deadline = Date.now() + 10_000
      for (;;) {
        ensure(Date.now() < deadline, 'the listener reconnects within 10 s')
        const wakeup = await listener.next().catch(() => null)
        if (wakeup?.kind === 'connected') {
          break
        }
        ensure(wakeup === null || wakeup.kind === 'poll', `before the reconnect: ${show(wakeup)}`)
      }
      await journaled.commit({entityId: 'a', events: [ledgerEvent('a', 2)]})
      ensureWakeup(await listener.next(), {kind: 'scope', scope: 'ledger:a'}, 'heard again')

      // Steady notifications never starve the backstop.
      let polled = false
      for (let sequence = 3; sequence < 13; sequence++) {
        await journaled.commit({entityId: 'a', events: [ledgerEvent('a', sequence)]})
        await delay(150)
        for (;;) {
          const wakeup = await listener.next({signal: AbortSignal.timeout(50)}).catch(() => null)
          if (wakeup === null) {
            break
          }
          polled ||= wakeup.kind === 'poll'
        }
      }
      ensure(polled, 'a poll arrives at its cadence under steady traffic')

      // Nor does a backlog: one commit wakes 30 scopes, and once the poll is overdue it comes
      // first, then every scope.
      const fanning = new JournalRepository(
        repo,
        new Journal(['ledger'], (event) =>
          Array.from({length: 30}, (_, i) => `fan:${event.entityId}:${i}`),
        ),
      )
      while ((await listener.next()).kind !== 'poll') {
        // Drains to the next poll.
      }
      await fanning.commit({entityId: 'c', events: [ledgerEvent('c', 1)]})
      await delay(700)
      ensureWakeup(await listener.next(), {kind: 'poll'}, 'the overdue poll comes first')
      let woken = 0
      for (;;) {
        const wakeup = await listener.next({signal: AbortSignal.timeout(200)}).catch(() => null)
        if (wakeup === null) {
          break
        }
        woken += wakeup.kind === 'scope' ? 1 : 0
      }
      ensure(woken === 30, `every scope of the backlog follows: got ${woken}`)
    } finally {
      await listener.close()
    }
  })
}

function ensureWakeup(got: Wakeup, want: Wakeup, what: string): void {
  ensure(show(got) === show(want), `${what}: got ${show(got)}, want ${show(want)}`)
}

function show(wakeup: Wakeup | null): string {
  return JSON.stringify(wakeup)
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

// A view row owned by `owner`, of entity type `kind`.
function viewRow(pk: string, sk: string, payload = '{}'): ViewRow {
  return {pk, sk, entityId: 'owner', entityType: 'kind', payload: new TextEncoder().encode(payload)}
}

function decode(payload: Uint8Array | undefined): string {
  return new TextDecoder().decode(payload)
}

// Incompressible printable ASCII from a fixed seed, as Rust's case builds its boundary key.
function noiseSource(): (length: number) => string {
  let seed = 0x9e3779b97f4a7c15n

  return (length) => {
    let out = ''
    for (let i = 0; i < length; i++) {
      seed = (seed * 6364136223846793005n + 1n) & 0xffffffffffffffffn
      out += String.fromCharCode(33 + Number((seed >> 33n) % 94n))
    }

    return out
  }
}

// Fails unless the call rejects with an EvtError of `code` whose message holds `text`.
async function refusesWith(call: Promise<unknown>, code: string, text: string, what: string) {
  try {
    await call
  } catch (error) {
    const {code: got, message} = error as {code?: unknown; message?: unknown}
    ensure(got === code && String(message).includes(text), `${what}: got ${String(error)}`)

    return
  }

  throw new Error(`${what}: resolved, want ${code}`)
}

// The compat view writer with RETURNING on its puts and present deletes, which makes their count
// driver-independent: one that returns nothing affected nothing.
function returning(views: string): ViewWriter {
  const writer = postgresViewWriter(views)

  return (op) => {
    const statement = writer(op)
    const counted = op.kind === 'put' ? op.condition !== 'absent' : op.condition === 'present'
    return counted
      ? {...statement, sql: `${statement.sql} RETURNING 1`, returning: true}
      : statement
  }
}

// A driver's result with its rows alone.
async function rowsOnly(result: Promise<{rows: unknown[]}>): Promise<{rows: unknown[]}> {
  return {rows: (await result).rows}
}

// The pool as a driver that returns rows and reports no affected-row count.
function countless(): PgClient {
  return {
    query: (text, values) => rowsOnly(pool.query(text, values)),
    async connect() {
      const connection = await pool.connect()
      return {
        query: (text: string, values?: unknown[]) => rowsOnly(connection.query(text, values)),
        release: (error?: Error) => connection.release(error),
      }
    },
  }
}

// A view operation of the `handles` projector.
function projected(op: ViewOperation, operation = 0): ProjectedView {
  return {projector: 'handles', projectorIndex: 0, operation, op}
}

// Fails unless `call` rejects with a ProjectorOperationError with `want` as its reason.
async function reason(call: Promise<unknown>, want: string, what: string): Promise<void> {
  try {
    await call
  } catch (error) {
    ensure(error instanceof ProjectorOperationError, `${what}: got ${String(error)}`)
    ensure(error.reason === want, `${what}: the reason ${error.reason}, want ${want}`)

    return
  }

  throw new Error(`${what}: resolved, want a ProjectorOperationError`)
}

// The log's marker and rebuild-fence rows, written straight to its tables.
function controls(repo: PgRepository): ControlFixtures {
  return postgresControls(pool, repo.tables.events)
}

function repoTables(repo: PgRepository): {eventsTable: string; snapshotsTable: string} {
  return {eventsTable: repo.tables.events, snapshotsTable: repo.tables.snapshots}
}

function ledgerEvent(entityId: string, sequence: number): SerializedEvent {
  return {
    id: eventId(entityId, BigInt(sequence)),
    sequence: BigInt(sequence),
    type: 'ledger:added',
    version: 1,
    entityId,
    entityType: 'ledger',
    payload: new TextEncoder().encode('{}'),
    metadata: {...emptyMetadata(), timestamp: '2026-01-02T03:04:05Z'},
  }
}

function ensure(condition: boolean, what: string): asserts condition {
  if (!condition) {
    throw new Error(what)
  }
}

// Fails unless `call` rejects with an error of `type`.
async function refuses(
  call: Promise<unknown>,
  type: abstract new (...args: never[]) => Error,
  what: string,
): Promise<void> {
  try {
    await call
  } catch (error) {
    ensure(error instanceof type, `${what}: got ${String(error)}`)

    return
  }

  throw new Error(`${what}: resolved, want a ${type.name}`)
}

// Writes the rows no repository call can, straight to the tables.
function headFixtures(repo: PgRepository): HeadFixtures {
  const {events} = repo.tables
  const heads = `${events}__evt_stream_heads`

  return {
    async appendBypassingHead(event: SerializedEvent) {
      const payload = event.payload ?? new Uint8Array()
      await pool.query(
        `INSERT INTO ${events} ` +
          '(entity_id, sequence, event_id, event_type, version, entity_type, payload, metadata) ' +
          'VALUES ($1, $2::bigint, $3, $4, $5::bigint, $6, $7, $8::text::jsonb)',
        [
          event.entityId,
          event.sequence.toString(),
          event.id,
          event.type,
          event.version.toString(),
          event.entityType,
          payload,
          encodeMetadata(event.metadata),
        ],
      )
    },
    async deleteHead(entityId: string) {
      await pool.query(`DELETE FROM ${heads} WHERE entity_id = $1`, [entityId])
    },
    async storedHead(entityId: string) {
      const {rows} = await pool.query<{head: string}>(
        `SELECT head::text AS head FROM ${heads} WHERE entity_id = $1`,
        [entityId],
      )

      return rows[0] === undefined ? null : BigInt(rows[0].head)
    },
  }
}
