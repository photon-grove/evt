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
import {
  ALLOCATION_KEY,
  COUNTER_SORT_KEY,
  DEFAULT_MAX_ROW_BYTES,
  Journal,
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
