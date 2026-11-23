// journal.json, which Rust generates (ADR 0005): the key layout, the allocation extension, the
// rows and counters the fixture's stored events replay to, the widest-row measure, and the marker
// a journaled store declares. TypeScript only reads the fixture.
import {
  type FeatureManifest,
  type JsonValue,
  type SerializedEvent,
  decodeEvent,
  encodeEvent,
  encodeManifest,
  groupCommits,
  parseJson,
  projectViews,
  stringifyJson,
} from '@photon-grove/evt'
import {markerItem, viewRowItem} from '@photon-grove/evt/dynamodb'
import {MemRepository} from '@photon-grove/evt/memory'
import {
  ALLOCATION_KEY,
  COUNTER_SORT_KEY,
  DEFAULT_MAX_ROW_BYTES,
  Journal,
  JournalGoneError,
  LEASE_SORT_KEY,
  MANIFEST_SORT_KEY,
  MAX_CHUNK_BYTES,
  MAX_CHUNK_ROWS,
  auditSortKey,
  chunkSortKey,
  compactWithLease,
  compactionPartitionKey,
  decodeChunk,
  decodeManifest,
  manifestDigest,
  page,
  JournalRepository,
  MAX_SCOPE_BYTES,
  POSITION_WIDTH,
  allocations,
  counterPartitionKey,
  counterView,
  encodeAllocations,
  journalProjector,
  journalRowFromView,
  parseRowSortKey,
  replay,
  rowPartitionKey,
  rowSize,
  rowSortKey,
} from '@photon-grove/evt/journal'

import {type Checks, canonical, field, list, loadFixture, num, plain, str} from './harness.js'

export async function journalVectors(checks: Checks): Promise<void> {
  const fixture = await loadFixture('journal.json')

  const limits = field(fixture, 'limits')
  checks.equal('journal limits maxScopeBytes', MAX_SCOPE_BYTES, num(field(limits, 'maxScopeBytes')))
  checks.equal('journal limits positionWidth', POSITION_WIDTH, num(field(limits, 'positionWidth')))
  checks.equal(
    'journal limits defaultMaxRowBytes',
    DEFAULT_MAX_ROW_BYTES,
    num(field(limits, 'defaultMaxRowBytes')),
  )

  for (const key of list(field(fixture, 'keys'))) {
    const scope = str(field(key, 'scope'))
    const position = at(field(key, 'position'))
    const name = `journal key ${scope} at ${position}`
    checks.equal(`${name} rowPk`, rowPartitionKey(scope), str(field(key, 'rowPk')))
    checks.equal(`${name} rowSk`, rowSortKey(position), str(field(key, 'rowSk')))
    checks.equal(`${name} counterPk`, counterPartitionKey(scope), str(field(key, 'counterPk')))
    checks.equal(`${name} counterSk`, COUNTER_SORT_KEY, str(field(key, 'counterSk')))
    checks.equal(`${name} parses back`, parseRowSortKey(str(field(key, 'rowSk'))), position)
  }

  for (const vector of list(field(fixture, 'allocations'))) {
    const name = `journal allocations ${str(field(vector, 'name'))}`
    checks.run(name, () => {
      const given = list(field(vector, 'allocations')).map((allocation) => ({
        scope: str(field(allocation, 'scope')),
        position: at(field(allocation, 'position')),
      }))
      const sorted = [...given]
      sorted.sort((a, b) => compareBytes(utf8(a.scope), utf8(b.scope)))
      const value = str(field(vector, 'extensionValue'))
      checks.equal(`${name} key`, ALLOCATION_KEY, str(field(vector, 'extensionKey')))
      checks.equal(`${name} encoded`, encodeAllocations(sorted), value)
      const read = allocations(metadataWith(value))
      checks.equal(`${name} read back`, canonical(read.map(text)), canonical(sorted.map(text)))
    })
  }

  commitVectors(checks, field(fixture, 'commit'))
  await liveCommitVectors(checks, field(fixture, 'commit'), field(fixture, 'compaction'))

  const marker = field(fixture, 'marker')
  checks.run('journal marker', () => {
    const manifest: FeatureManifest = {
      version: 2n,
      features: new Map([
        ['journal', 1],
        ['stream-heads', 1],
      ]),
      historyDependent: ['account'],
      retention: [],
    }
    const texts = encodeManifest(manifest)
    checks.equal('journal marker features', texts.features, str(field(marker, 'features')))
    checks.equal(
      'journal marker historyDependent',
      texts.historyDependent,
      str(field(marker, 'historyDependent')),
    )
    checks.equal(
      'journal marker item',
      canonical(markerItem(manifest)),
      canonical(plain(field(marker, 'dynamoItem'))),
    )
  })
  checks.run('journal compaction marker', () => {
    const features = new Map([
      ['journal', 1],
      ['journal-compaction', 1],
      ['stream-heads', 1],
    ])
    const manifest = {version: 1n, features, historyDependent: [], retention: []}
    checks.equal(
      'journal compaction marker features',
      encodeManifest(manifest).features,
      str(field(field(fixture, 'compactionMarker'), 'features')),
    )
  })
}

// The stored events of the fixture's commits replay, in any order, to its rows and counters.
function commitVectors(checks: Checks, commit: JsonValue): void {
  const events = list(field(commit, 'events')).map((event) => decodeEvent(stringifyJson(event)))
  events.forEach((event, index) => {
    checks.equal(
      `journal commit event ${event.id} re-encodes`,
      canonical(parseJson(encodeEvent(event))),
      canonical(list(field(commit, 'events'))[index]),
    )
  })

  // Last to first: the replay needs no order.
  const state = replay(events.map((_, index) => events[events.length - 1 - index]!))
  const rows = list(field(commit, 'rows'))
  checks.equal('journal commit row count', state.rows.length, rows.length)
  for (const want of rows) {
    const scope = str(field(want, 'scope'))
    const position = at(field(want, 'position'))
    const name = `journal commit row ${scope} at ${position}`
    checks.run(name, () => {
      const got = state.rows.find((row) => row.scope === scope && row.position === position)
      if (got === undefined) {
        throw new Error('the replay has no such row')
      }
      checks.equal(
        `${name} item`,
        canonical(viewRowItem(got.row)),
        canonical(plain(field(want, 'dynamoItem'))),
      )
      checks.equal(`${name} size`, got.row.payload.length, num(field(want, 'size')))
      checks.equal(`${name} owner`, got.row.entityId, str(field(want, 'owner')))
      checks.equal(`${name} decodes`, journalRowFromView(scope, got.row).position, position)
    })
  }

  const counters = list(field(commit, 'counters'))
  checks.equal('journal commit counter count', state.counters.length, counters.length)
  for (const want of counters) {
    const scope = str(field(want, 'scope'))
    const position = at(field(want, 'position'))
    const name = `journal commit counter ${scope}`
    const got = state.counters.find((counter) => counter.scope === scope)
    checks.equal(`${name} position`, got?.position, position)
    checks.equal(
      `${name} item`,
      canonical(viewRowItem(counterView(scope, position))),
      canonical(plain(field(want, 'dynamoItem'))),
    )
  }

  // The engine's regroup-and-replay of the journal's projector reproduces the rows too.
  checks.run('journal projector replays the rows', () => {
    const journal = new Journal(['account'], () => [])
    const projected = groupCommits(events).flatMap((view) =>
      projectViews([journalProjector(journal)], view),
    )
    const items = projected.map(({op}) =>
      viewRowItem({
        pk: op.pk,
        sk: op.sk,
        entityId: op.owner,
        entityType: op.entityType,
        payload: op.payload,
      }),
    )
    // The projector writes in event order; compare as sets.
    checks.equal(
      'journal projector rows',
      sortedTexts(items).join('\n'),
      sortedTexts(state.rows.map(({row}) => viewRowItem(row))).join('\n'),
    )
  })

  const measured = field(commit, 'measuredRowSize')
  const event = events.find((candidate) => candidate.id === str(field(measured, 'event')))!
  checks.equal(
    'journal measured row size',
    rowSize(str(field(measured, 'scope')), at(field(measured, 'position')), event),
    num(field(measured, 'bytes')),
  )
}

// A position, which the fixture writes as a decimal string so a u64 survives every JSON reader.
function at(value: JsonValue): bigint {
  return BigInt(str(value))
}

// Documents as sorted canonical texts, to compare as sets.
function sortedTexts(values: unknown[]): string[] {
  const texts = values.map(canonical)
  texts.sort()

  return texts
}

// The fixture's commits, made again through a journal over the in-memory repository: two events
// of one stream, then one of another, into a world scope and a place scope of each stream. The
// stored events, rows and counters must be the fixture's, byte for byte.
async function liveCommitVectors(
  checks: Checks,
  commit: JsonValue,
  compaction: JsonValue,
): Promise<void> {
  await checks.case('journal commit through the journal repository', async () => {
    // A fixed clock, so the lease expiry and the audit's completion time are stable.
    const memory = new MemRepository({clock: () => FIXTURE_CLOCK_MS})
    const journal = new Journal(['account'], (event) => [
      'world:7',
      `world:7/${event.entityId === 'acct-1' ? 'place:3' : 'place:4'}`,
    ])
    const journaled = new JournalRepository(memory, journal)
    const drafted = list(field(commit, 'events')).map((event) => {
      const decoded = decodeEvent(stringifyJson(event))
      // The fixture's events as drafted: no allocation yet.
      decoded.metadata = {...decoded.metadata, extensions: new Map()}
      return decoded
    })
    await journaled.commit({entityId: 'acct-1', events: drafted.slice(0, 2)})
    await journaled.commit({entityId: 'acct-2', events: drafted.slice(2)})

    const stored = [...(await memory.events('acct-1')), ...(await memory.events('acct-2'))]
    const want = list(field(commit, 'events'))
    if (canonical(stored.map((event) => parseJson(encodeEvent(event)))) !== canonical(want)) {
      throw new Error('the stored events differ from the fixture')
    }

    const live = memory
      .views()
      .filter((row) => row.pk.startsWith('journal#') || row.pk.startsWith('journal-counter#'))
    const items = sortedTexts(live.map((row) => viewRowItem(row)))
    const fixture = sortedTexts([
      ...list(field(commit, 'rows')).map((row) => plain(field(row, 'dynamoItem'))),
      ...list(field(commit, 'counters')).map((row) => plain(field(row, 'dynamoItem'))),
    ])
    if (items.join('\n') !== fixture.join('\n')) {
      throw new Error(`the live rows differ from the fixture:\n${items.join('\n')}`)
    }

    await compactionVectors(checks, memory, compaction)
  })
}

// world:7 compacted through 2 by compactor-1 under a 60-second lease, then read after 1.
async function compactionVectors(
  checks: Checks,
  memory: MemRepository,
  compaction: JsonValue,
): Promise<void> {
  const world = 'world:7'
  const report = await compactWithLease(memory, world, 2n, 'compactor-1', 60_000)
  checks.equal('journal compaction digest', report.digest, str(field(compaction, 'manifestDigest')))

  const keys = field(compaction, 'keys')
  checks.equal('journal compaction pk', compactionPartitionKey(world), str(field(keys, 'pk')))
  checks.equal('journal compaction lease key', LEASE_SORT_KEY, str(field(keys, 'lease')))
  checks.equal('journal compaction manifest key', MANIFEST_SORT_KEY, str(field(keys, 'manifest')))
  checks.equal('journal compaction chunk key', chunkSortKey(0), str(field(keys, 'chunk0')))
  checks.equal('journal compaction audit key', auditSortKey(2n), str(field(keys, 'audit2')))
  const limits = field(compaction, 'limits')
  checks.equal('journal compaction max rows', MAX_CHUNK_ROWS, num(field(limits, 'maxChunkRows')))
  checks.equal('journal compaction max bytes', MAX_CHUNK_BYTES, num(field(limits, 'maxChunkBytes')))

  const stored = [
    ['lease', LEASE_SORT_KEY, field(compaction, 'lease')],
    ['manifest', MANIFEST_SORT_KEY, field(compaction, 'manifest')],
    ['chunk 0', chunkSortKey(0), list(field(compaction, 'chunks'))[0]!],
    ['audit', auditSortKey(2n), field(compaction, 'audit')],
  ] as const
  const rows = await Promise.all(stored.map(([, sk]) => memory.readCompactionRow(world, sk)))
  for (const [index, [name, , want]] of stored.entries()) {
    const row = rows[index] ?? null
    checks.equal(
      `journal compaction ${name} item`,
      row && canonical(viewRowItem(row)),
      canonical(plain(want)),
    )
  }
  checks.equal(
    'journal compaction remaining rows',
    canonical((await memory.readRows(world, 0n, 10)).map((row) => row.position.toString())),
    canonical(plain(field(compaction, 'remainingRows'))),
  )

  const gone = field(compaction, 'gone')
  let handoff: unknown
  try {
    await page(memory, world, 1n, 10)
  } catch (error) {
    handoff = error
  }
  if (!(handoff instanceof JournalGoneError)) {
    throw new Error(`a page below the floor: ${String(handoff)}`)
  }
  checks.equal('journal gone status', JournalGoneError.HTTP_STATUS, num(field(gone, 'httpStatus')))
  checks.equal('journal gone after', handoff.after, at(field(gone, 'after')))
  checks.equal('journal gone checkpoint', handoff.checkpoint, at(field(gone, 'checkpoint')))
  checks.equal('journal gone chunks', handoff.chunkCount, num(field(gone, 'chunkCount')))
  checks.equal('journal gone digest', handoff.digest, str(field(gone, 'digest')))
  checks.equal('journal gone resume', handoff.resumeAfter, at(field(gone, 'resumeAfter')))

  // The stored rows decode back: the manifest and its digest, and the chunk's rows 1 and 2.
  const manifestText = str(field(field(field(compaction, 'manifest'), 'payload'), 'S'))
  const manifest = decodeManifest(world, new TextEncoder().encode(manifestText))
  checks.equal('journal manifest decodes', await manifestDigest(manifest), handoff.digest)
  const chunkText = str(field(field(list(field(compaction, 'chunks'))[0]!, 'payload'), 'S'))
  const chunk = decodeChunk(world, new TextEncoder().encode(chunkText))
  checks.equal('journal chunk range', `${chunk.from}..${chunk.through}`, '1..2')
}

// The fixture's clock: 1,792,000,000 Unix seconds.
const FIXTURE_CLOCK_MS = 1_792_000_000_000

function metadataWith(value: string): SerializedEvent['metadata'] {
  const event = decodeEvent('{}')

  return {...event.metadata, extensions: new Map([[ALLOCATION_KEY, value]])}
}

function text(allocation: {scope: string; position: bigint}): string {
  return `${allocation.scope}@${allocation.position}`
}

function utf8(value: string): Uint8Array {
  return new TextEncoder().encode(value)
}

function compareBytes(a: Uint8Array, b: Uint8Array): number {
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    if (a[i] !== b[i]) {
      return a[i]! - b[i]!
    }
  }

  return a.length - b.length
}
