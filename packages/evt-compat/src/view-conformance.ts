// The view repository conformance cases, the TypeScript port of Rust's `view_repository_cases`
// (crates/evt/src/conformance/view_repository.rs): point reads and writes, listings by partition
// and entity type, cursors, batch writes and reads, and the refusals. Each case namespaces its keys
// and entity type, so a durable table can be shared across runs. Each step needs the one before
// it, so the steps await in loops on purpose.
/* oxlint-disable no-await-in-loop */
import type {ViewPage, ViewRepository, ViewRow} from '@photon-grove/evt'

import type {Checks} from './harness.js'

const encoder = new TextEncoder()
const decoder = new TextDecoder()

let namespaces = 0

/** Runs every case against `repo`, under `label` (the backend's name). */
export async function viewRepositoryConformance(
  checks: Checks,
  label: string,
  repo: ViewRepository,
): Promise<void> {
  const cases: [string, (repo: ViewRepository) => Promise<void>][] = [
    ['a view is put, read, overwritten and deleted', roundTrip],
    ['an empty sort key is stored as VIEW, and a ttl round-trips', defaultSkAndTtl],
    ["rows of a partition list ascending by sort key bytes, and only that partition's", listByPk],
    ['a partition pages with a cursor until it is exhausted', pageByPk],
    ['an entity type lists and pages across partitions', byEntityType],
    ['a malformed cursor, or one from another listing, is refused', badCursor],
    ['putViews writes every row, in chunks, and nothing from an empty list', putMany],
    ['batchGetViews de-duplicates, skips empty and missing keys, and chunks', batchGet],
    ['a row with an empty pk or a non-UTF-8 payload is refused before any write', refusals],
    [
      'U+0000 or a lone surrogate in a row or a named key is refused; a batch holding one writes nothing',
      nul,
    ],
  ]

  for (const [name, run] of cases) {
    await checks.case(`${label} views: ${name}`, () => run(repo))
  }
}

function namespace(): string {
  return `views-${Date.now().toString(36)}-${++namespaces}`
}

function row(pk: string, sk: string, entityType: string, payload: string, ttl = 0): ViewRow {
  return {
    pk,
    sk,
    entityId: `owner-${pk}`,
    entityType,
    payload: encoder.encode(payload),
    ttl,
  }
}

// A row as comparable text, its payload as a string and a missing ttl as 0.
function show(viewRow: ViewRow | null | undefined): string {
  if (viewRow === null || viewRow === undefined) {
    return String(viewRow)
  }

  const {pk, sk, entityId, entityType, payload, ttl} = viewRow

  return JSON.stringify([pk, sk, entityId, entityType, decoder.decode(payload), ttl ?? 0])
}

function showAll(rows: readonly ViewRow[]): string {
  return `[${rows.map(show).join(', ')}]`
}

function keys(rows: readonly ViewRow[]): string[] {
  return rows.map((viewRow) => `${viewRow.pk}/${viewRow.sk}`)
}

async function roundTrip(repo: ViewRepository): Promise<void> {
  const ns = namespace()
  const pk = `${ns}-a`
  const first = row(pk, 'VIEW', `${ns}-t`, '{"v":1}')

  equal(show(await repo.getView(pk)), 'null', 'an absent view')

  await repo.putView(first)
  equal(show(await repo.getView(pk)), show(first), 'the row')
  equal(show(await repo.getViewAt(pk, 'VIEW')), show(first), 'the row at its sort key')

  const second = {
    ...row(pk, 'VIEW', `${ns}-u`, '{"v":2}'),
    entityId: 'owner-2',
  }
  await repo.putView(second)
  equal(show(await repo.getView(pk)), show(second), 'a put replaces the whole row')

  await repo.deleteView(pk, '')
  equal(show(await repo.getView(pk)), 'null', 'an empty sort key deletes VIEW')
  await repo.deleteView(pk, 'VIEW')
  await repo.deleteView(`${ns}-never`, 'x')
}

async function defaultSkAndTtl(repo: ViewRepository): Promise<void> {
  const ns = namespace()
  const pk = `${ns}-a`

  const put = row(pk, '', `${ns}-t`, '{}', 4_102_444_800)
  await repo.putView(put)
  equal(show(await repo.getView(pk)), show({...put, sk: 'VIEW'}), 'stored at VIEW')

  // Replacing a row without a ttl removes the ttl.
  await repo.putView(row(pk, 'VIEW', `${ns}-t`, '{}'))
  equal(String((await repo.getView(pk))?.ttl), '0', 'the ttl is gone')
}

async function listByPk(repo: ViewRepository): Promise<void> {
  const ns = namespace()
  const [pk, other] = [`${ns}-a`, `${ns}-b`]

  equal(showAll(await repo.listViewsByPk(pk)), '[]', 'an empty partition')

  // UTF-8 byte order: "a" < "z" < "é" (C3 A9) < U+10000 (F0 90 80 80), which UTF-16 order isn't.
  for (const sk of ['é', 'a', '\u{10000}', 'z', '￿']) {
    await repo.putView(row(pk, sk, `${ns}-t`, '{}'))
  }
  await repo.putView(row(other, 'm', `${ns}-t`, '{}'))

  equal(
    keys(await repo.listViewsByPk(pk)).join(' '),
    ['a', 'z', 'é', '￿', '\u{10000}'].map((sk) => `${pk}/${sk}`).join(' '),
    'ascending by sort key bytes',
  )
}

async function pageByPk(repo: ViewRepository): Promise<void> {
  const ns = namespace()
  const pk = `${ns}-a`
  const all = [0, 1, 2, 3, 4].map((index) => row(pk, `s${index}`, `${ns}-t`, '{}'))
  await repo.putViews(all)

  const seen: ViewRow[] = []
  let cursor: string | null = null
  let pages = 0
  do {
    const page: ViewPage = await repo.listViewsByPkPaged(pk, {
      limit: 2,
      cursor,
    })
    ensure(page.views.length <= 2, `a page of ${page.views.length} rows exceeds the limit`)
    seen.push(...page.views)
    pages++
    ensure(pages <= 6, 'the pages never end')
    cursor = page.nextCursor
  } while (cursor !== null)

  equal(showAll(seen), showAll(all), 'the pages, in order, are the whole partition')
  ensure(pages >= 3, 'five rows at two per page take at least three pages')

  const unbounded = await repo.listViewsByPkPaged(pk)
  equal(showAll(unbounded.views), showAll(all), 'an unbounded page')
  equal(String(unbounded.nextCursor), 'null', 'nothing follows')

  for (const limit of [0, -3]) {
    const page = await repo.listViewsByPkPaged(pk, {limit})
    equal(showAll(page.views), showAll(all), `a limit of ${limit} is no limit`)
  }
  await refuses(repo.listViewsByPkPaged(pk, {limit: 1.5}), 'invalidArgument', 'a fractional limit')
}

async function byEntityType(repo: ViewRepository): Promise<void> {
  const ns = namespace()
  const entityType = `${ns}-t`

  const all: ViewRow[] = []
  for (const partition of [0, 1]) {
    for (const index of [0, 1, 2]) {
      all.push(row(`${ns}-p${partition}`, `s${index}`, entityType, '{}'))
    }
  }
  await repo.putViews([...all, row(`${ns}-other`, 's', `${ns}-u`, '{}')])
  const want = sortedCopy(keys(all)).join(' ')

  const listed = await repo.listViewsByEntityType(entityType)
  equal(sortedCopy(keys(listed)).join(' '), want, 'every row of the type, and no other')
  equal(showAll(await repo.listViewsByEntityType(`${ns}-none`)), '[]', 'a type with no rows')

  const seen: string[] = []
  let cursor: string | null = null
  let pages = 0
  do {
    const page: ViewPage = await repo.listViewsByEntityTypePaged(entityType, {
      limit: 2,
      cursor,
    })
    ensure(page.views.length <= 2, 'a page exceeds the limit')
    seen.push(...keys(page.views))
    pages++
    ensure(pages <= 8, 'the pages never end')
    cursor = page.nextCursor
  } while (cursor !== null)

  equal(String(seen.length), '6', 'no row is repeated or skipped across pages')
  equal(sortedCopy(seen).join(' '), want, 'the pages cover the type')
}

async function badCursor(repo: ViewRepository): Promise<void> {
  const ns = namespace()

  for (const cursor of [
    'not base64!',
    'bm90IGpzb24=',
    'eyJwayI6MX0=',
    'e30=',
    'eyJwayI6InAiLCJzayI6InMifQ',
  ]) {
    await refuses(
      repo.listViewsByPkPaged(ns, {limit: 1, cursor}),
      'invalidArgument',
      `pk listing with cursor ${cursor}`,
    )
    await refuses(
      repo.listViewsByEntityTypePaged(ns, {limit: 1, cursor}),
      'invalidArgument',
      `entity type listing with cursor ${cursor}`,
    )
  }

  // An empty cursor is the first page.
  await repo.listViewsByPkPaged(ns, {limit: 1, cursor: ''})

  // A cursor continues the listing that made it.
  const [pk, other, entityType] = [`${ns}-a`, `${ns}-b`, `${ns}-t`]
  await repo.putViews([row(pk, '1', entityType, '{}'), row(pk, '2', entityType, '{}')])

  const byPk = (await repo.listViewsByPkPaged(pk, {limit: 1})).nextCursor
  ensure(byPk !== null, 'a partition cursor')
  const byType = (await repo.listViewsByEntityTypePaged(entityType, {limit: 1})).nextCursor
  ensure(byType !== null, 'an entity type cursor')

  const foreign: [() => Promise<unknown>, string][] = [
    [
      () => repo.listViewsByPkPaged(other, {limit: 1, cursor: byPk}),
      'a partition cursor elsewhere',
    ],
    [
      () => repo.listViewsByPkPaged(other, {limit: 1, cursor: byType}),
      'a type cursor on a partition',
    ],
    [
      () => repo.listViewsByEntityTypePaged(entityType, {limit: 1, cursor: byPk}),
      'a partition cursor on an entity type',
    ],
    [
      () =>
        repo.listViewsByEntityTypePaged(`${ns}-u`, {
          limit: 1,
          cursor: byType,
        }),
      'an entity type cursor on another entity type',
    ],
  ]
  for (const [listing, what] of foreign) {
    await refuses(listing(), 'invalidArgument', what)
  }
}

async function putMany(repo: ViewRepository): Promise<void> {
  const ns = namespace()
  const pk = `${ns}-a`

  await repo.putViews([])

  // 60 rows are three BatchWriteItem calls (25, 25, 10) on DynamoDB.
  const all = Array.from({length: 60}, (_, index) =>
    row(pk, `s${String(index).padStart(2, '0')}`, `${ns}-t`, '{}'),
  )
  await repo.putViews(all)
  equal(showAll(await repo.listViewsByPk(pk)), showAll(all), 'every row is stored')

  // A key repeated within one batch keeps its last row.
  const repeated = `${ns}-repeat`
  await repo.putViews([
    row(repeated, 'VIEW', `${ns}-t`, '{"n":1}'),
    row(repeated, '', `${ns}-t`, '{"n":2}'),
    row(repeated, 'VIEW', `${ns}-t`, '{"n":3}'),
  ])
  const stored = await repo.getView(repeated)
  equal(decoder.decode(stored?.payload), '{"n":3}', 'the last of a repeated key')

  // The batch is a plain put: a later one replaces rows.
  const replaced = all
    .slice(0, 3)
    .map((viewRow) => row(viewRow.pk, viewRow.sk, viewRow.entityType, '{"again":true}'))
  await repo.putViews(replaced)
  const listed = await repo.listViewsByPk(pk)
  equal(showAll(listed.slice(0, 3)), showAll(replaced), 'replaced rows')
  equal(String(listed.length), '60', 'no row was added')
}

async function batchGet(repo: ViewRepository): Promise<void> {
  const ns = namespace()
  const entityType = `${ns}-t`

  // 130 distinct partitions take two BatchGetItem calls (100, 30).
  const all = Array.from({length: 130}, (_, index) =>
    row(`${ns}-p${String(index).padStart(3, '0')}`, 'VIEW', entityType, '{}'),
  )
  await repo.putViews(all)

  const pks = [...all.map((viewRow) => viewRow.pk), all[0]!.pk, '', 'missing-partition']
  const got = await repo.batchGetViews(pks)
  equal(showAll(sortedCopy(got, comparePk)), showAll(all), 'each existing row once')
  equal(showAll(await repo.batchGetViews([])), '[]', 'no keys')

  const custom = row(all[0]!.pk, 'other', entityType, '{}')
  await repo.putView(custom)
  equal(
    showAll(await repo.batchGetViewsAt([all[0]!.pk, all[1]!.pk], 'other')),
    showAll([custom]),
    'a custom sort key',
  )
}

async function refusals(repo: ViewRepository): Promise<void> {
  const ns = namespace()
  const pk = `${ns}-a`

  await refuses(repo.putView(row('', 's', ns, '{}')), 'invalidArgument', 'an empty pk')

  const bad: ViewRow = {
    ...row(pk, 's', ns, ''),
    payload: new Uint8Array([0xff, 0xfe]),
  }
  await refuses(repo.putView(bad), 'invalidRecord', 'a non-UTF-8 payload')
  await refuses(repo.putView({...row(pk, 's', ns, '{}'), ttl: 1.5}), 'invalidArgument', 'a ttl')
  await refuses(repo.getViewAt('', 's'), 'invalidArgument', 'a read with an empty pk')
  await refuses(repo.deleteView('', 's'), 'invalidArgument', 'a delete with an empty pk')

  // An empty sort key reads the single-item row, as it writes it.
  await repo.putView(row(pk, '', ns, '{}'))
  ensure((await repo.getViewAt(pk, '')) !== null, 'an empty sort key reads VIEW')
  equal(String((await repo.batchGetViewsAt([pk], '')).length), '1', 'a batch read at VIEW')
  await repo.deleteView(pk, 'VIEW')

  // A bad row anywhere in a batch refuses the whole batch before any write.
  await refuses(repo.putViews([row(pk, 'good', ns, '{}'), bad]), 'invalidRecord', 'a bad batch')
  equal(showAll(await repo.listViewsByPk(pk)), '[]', 'nothing was written')
}

async function nul(repo: ViewRepository): Promise<void> {
  const ns = namespace()
  const [pk, entityType] = [`${ns}-a`, `${ns}-t`]

  // PostgreSQL text can't hold U+0000, so every backend refuses it, in each text of a row.
  const rows = [
    row(`${pk}\0`, 'VIEW', entityType, '{}'),
    row(pk, 's\0k', entityType, '{}'),
    {...row(pk, 'VIEW', entityType, '{}'), entityId: 'own\0er'},
    row(pk, 'VIEW', `${entityType}\0`, '{}'),
    row(pk, 'VIEW', entityType, '{"a":"\0"}'),
  ]
  for (const bad of rows) {
    await refuses(repo.putView(bad), 'invalidArgument', show(bad))
  }

  await refuses(
    repo.putViews([row(pk, 'good', entityType, '{}'), rows[4]!]),
    'invalidArgument',
    'a batch with U+0000 in a payload',
  )
  equal(showAll(await repo.listViewsByPk(pk)), '[]', 'nothing was written')

  const nulPk = `${pk}\0`
  const reads: [() => Promise<unknown>, string][] = [
    [() => repo.getViewAt(nulPk, 'VIEW'), 'a read of such a pk'],
    [() => repo.getViewAt(pk, 's\0k'), 'a read of such an sk'],
    [() => repo.deleteView(nulPk, 'VIEW'), 'a delete of such a pk'],
    [() => repo.deleteView(pk, 's\0k'), 'a delete of such an sk'],
    [() => repo.batchGetViewsAt([], 's\0k'), 'a batch read at such an sk, with no key'],
    [() => repo.batchGetViews([pk, nulPk]), 'a batch read naming such a pk'],
    [() => repo.listViewsByPkPaged(nulPk), 'a listing of such a pk'],
    [() => repo.listViewsByEntityTypePaged(`${entityType}\0`), 'a listing of such an entity type'],
  ]
  for (const [read, what] of reads) {
    await refuses(read(), 'invalidArgument', what)
  }

  // A lone surrogate isn't UTF-8: Go would store U+FFFD in its place, and two keys would collide.
  const lone = `${pk}\ud800`
  const surrogates: [() => Promise<unknown>, string][] = [
    [() => repo.putView(row(lone, 'VIEW', entityType, '{}')), 'a put with such a pk'],
    [() => repo.putView(row(pk, 'a\udc00', entityType, '{}')), 'a put with such an sk'],
    [() => repo.putView({...row(pk, 'VIEW', entityType, '{}'), entityId: '\ud800'}), 'an owner'],
    [() => repo.putView(row(pk, 'VIEW', `${entityType}\udc00`, '{}')), 'an entity type'],
    [() => repo.getViewAt(lone, 'VIEW'), 'a read of such a pk'],
    [() => repo.listViewsByPkPaged(lone), 'a listing of such a pk'],
    [() => repo.listViewsByEntityTypePaged(`${entityType}\ud800`), 'a listing of such a type'],
  ]
  for (const [call, what] of surrogates) {
    await refuses(call(), 'invalidArgument', `lone surrogate: ${what}`)
  }
  equal(showAll(await repo.listViewsByPk(pk)), '[]', 'nothing was written')
}

async function refuses(promise: Promise<unknown>, code: string, what: string): Promise<void> {
  try {
    await promise
  } catch (error) {
    const got = (error as {code?: unknown}).code
    ensure(got === code, `${what}: want ${code}, got ${String(got)} (${String(error)})`)
    return
  }

  throw new Error(`${what}: want ${code}, but it succeeded`)
}

function comparePk(a: ViewRow, b: ViewRow): number {
  return a.pk < b.pk ? -1 : a.pk > b.pk ? 1 : 0
}

// A sorted copy; ES2022 has no toSorted.
function sortedCopy<T>(values: readonly T[], compare?: (a: T, b: T) => number): T[] {
  // oxlint-disable-next-line unicorn/no-array-sort -- it sorts a copy
  return [...values].sort(compare)
}

function equal(got: string, want: string, what: string): void {
  ensure(got === want, `${what}\n    got  ${got}\n    want ${want}`)
}

function ensure(condition: boolean, message: string): asserts condition {
  if (!condition) {
    throw new Error(message)
  }
}
