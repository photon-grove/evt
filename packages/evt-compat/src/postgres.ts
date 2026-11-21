// Runs the conformance suites against PostgreSQL at DATABASE_URL, by default the database
// infra/local-postgres creates for Go's suite, and only ever a local server. Each run creates its
// own event logs, with table names unique to the run, and drops them at the end, so it can share
// the database with the Go and Rust suites. Each step needs the one before it, so the steps await
// in loops on purpose.
/* oxlint-disable no-await-in-loop */
import process from 'node:process'
import {Pool} from 'pg'

import {
  type FeatureManifest,
  type ProjectedView,
  ProjectorError,
  ProjectorOperationError,
  type SerializedEvent,
  StreamHeadsNotMigratedError,
  UnsupportedFeatureError,
  type ViewOperation,
  decodeManifest,
  emptyMetadata,
  encodeManifest,
  encodeMetadata,
  eventId,
  putView,
} from '@photon-grove/evt'
import {type PgClient, PgRepository, type ViewWriter} from '@photon-grove/evt/postgres'

import {
  type ControlFixtures,
  type HeadFixtures,
  type ManifestDraft,
  type RebuildState,
  conformance,
  markerConformance,
} from './conformance.js'
import {Checks} from './harness.js'
import {postgresViewDdl, postgresViewWriter} from './pgviews.js'

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
  await markerConformance(checks, marked, controlFixtures(marked))

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
    const manifest = await controlFixtures(repo).manifest()
    ensure(manifest?.version === 1n, 'the first write declares version 1')
    ensure(manifest.features.get('stream-heads') === 1, 'it declares stream-heads')
  })

  await checks.case('postgres: initialize declares once, across concurrent calls', async () => {
    const repo = await freshLog()
    const repos = [0, 1, 2].map(() => new PgRepository(pool, repoTables(repo)))
    const declared = await Promise.all(repos.map((each) => each.initialize()))
    ensure(declared.filter(Boolean).length === 1, `one call declares: got ${declared}`)
    ensure(!(await repo.initialize()), 'initializing again declares nothing')
    ensure((await controlFixtures(repo).manifest())?.version === 1n, 'the version bumps once')
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
    ensure((await controlFixtures(repo).manifest()) === null, 'nothing was declared')
  })

  await checks.case('postgres: a cached writer validates a declaration as it commits', async () => {
    const repo = await freshLog()
    await repo.commit({entityId: 'a', events: [ledgerEvent('a', 1)]})

    // The commit reads the manifest under the shared lock, so a supported declaration needs no
    // retry, and an unsupported one refuses the next commit.
    await controlFixtures(repo).declare((draft) => draft.retention.push('session'))
    await repo.commit({entityId: 'a', events: [ledgerEvent('a', 2)]})
    await controlFixtures(repo).declare((draft) => draft.features.set('journal', 1))
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
    await controlFixtures(first).declare((draft) => draft.features.set('journal', 1))
    await controlFixtures(first).setRebuild({generation: 1n, lock: 'held'})

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

  await checks.case('postgres: a statement that returns rows counts them', async () => {
    const repo = await freshLog(pool, returning)
    const put = (entityId: string, owner: string) =>
      repo.commit({
        entityId,
        events: [ledgerEvent(entityId, 1)],
        views: [projected(putView('k', 'one', owner, 'ledger', `{"by":"${owner}"}`))],
      })
    await put('a', 'a')
    await reason(put('b', 'b'), 'ConditionalCheckFailed', "an overwrite of another owner's row")
  })
}

// The compat view writer with RETURNING on its puts, which makes their count driver-independent: a
// put that returns nothing failed.
function returning(views: string): ViewWriter {
  const writer = postgresViewWriter(views)

  return (op) => {
    const statement = writer(op)
    return op.kind === 'put' ? {...statement, sql: `${statement.sql} RETURNING 1`} : statement
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

// Writes the marker and rebuild-fence rows straight to the tables, as another writer would.
function controlFixtures(repo: PgRepository): ControlFixtures {
  const {events} = repo.tables
  const features = `${events}__evt_features`

  const manifest = async (): Promise<FeatureManifest | null> => {
    const {rows} = await pool.query<Record<string, string>>(
      'SELECT version::text AS version, features, history_dependent, retention ' +
        `FROM ${features} WHERE id = 0`,
    )
    const row = rows[0]

    return row === undefined
      ? null
      : decodeManifest(BigInt(row['version']!), {
          features: row['features']!,
          historyDependent: row['history_dependent']!,
          retention: row['retention']!,
        })
  }

  return {
    manifest,
    async declare(change: (draft: ManifestDraft) => void) {
      const stored = await manifest()
      const draft: ManifestDraft = {
        features: new Map(stored?.features),
        historyDependent: [...(stored?.historyDependent ?? [])],
        retention: [...(stored?.retention ?? [])],
      }
      change(draft)

      const texts = encodeManifest({...draft, version: 0n})
      const version = ((stored?.version ?? 0n) + 1n).toString()
      await pool.query(
        `INSERT INTO ${features} (id, version, features, history_dependent, retention) ` +
          'VALUES (0, $1::bigint, $2, $3, $4) ON CONFLICT (id) DO UPDATE SET ' +
          'version = EXCLUDED.version, features = EXCLUDED.features, ' +
          'history_dependent = EXCLUDED.history_dependent, retention = EXCLUDED.retention',
        [version, texts.features, texts.historyDependent, texts.retention],
      )

      // Read back as stored: each set sorted and deduplicated.
      return (await manifest())!
    },
    async setRebuild(state: RebuildState) {
      // PostgreSQL keeps no lock row for a force-release: the generation row's flag records it.
      const generation = state.generation.toString()
      await pool.query(
        `INSERT INTO ${events}__evt_rebuild_generation (id, value, force_released) ` +
          'VALUES (0, $1::bigint, $2) ON CONFLICT (id) DO UPDATE SET ' +
          'value = EXCLUDED.value, force_released = EXCLUDED.force_released',
        [generation, state.lock === 'forceReleased'],
      )
      await pool.query(`DELETE FROM ${events}__evt_rebuild_lock WHERE id = 0`)
      if (state.lock === 'held') {
        await pool.query(
          `INSERT INTO ${events}__evt_rebuild_lock (id, owner, generation, lease_expires_at) ` +
            "VALUES (0, 'conformance', $1::bigint, now() + interval '5 minutes')",
          [generation],
        )
      }
    },
  }
}
