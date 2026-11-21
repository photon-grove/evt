// The in-memory view repository: the reference the conformance suite checks the DynamoDB one
// against, and a test double for code that reads views. Every read, the entity-type listing
// included, sees every write that has returned.

import type {CallOptions} from '../repository.js'
import {compareUtf8} from '../utf8.js'
import {
  type PageOptions,
  type ViewPage,
  type ViewRepository,
  type ViewRow,
  allPages,
  batchKeys,
  checkViewRow,
  cursorKey,
  encodeViewCursor,
  nulFree,
  pageLimit,
  resolveViewKey,
  viewTtl,
} from '../views.js'

/**
 * A {@link ViewRepository} in memory. Rows are kept as written, `ttl` included, and never
 * expire. A partition lists ascending by `sk` bytes and an entity type by `pk` bytes, then `sk`
 * bytes. A page carries a cursor only when a row follows it, in the same format DynamoDB's
 * cursors have.
 */
export class MemViewRepository implements ViewRepository {
  // The rows by pk and then sk.
  readonly #rows = new Map<string, Map<string, ViewRow>>()

  async getView(pk: string, options?: CallOptions): Promise<ViewRow | null> {
    return this.getViewAt(pk, '', options)
  }

  async getViewAt(pk: string, sk: string, options?: CallOptions): Promise<ViewRow | null> {
    options?.signal?.throwIfAborted()
    // Resolved first: optional chaining would skip the key's checks for an absent partition.
    const key = resolveViewKey(pk, sk)
    const row = this.#rows.get(pk)?.get(key)

    return row === undefined ? null : clone(row)
  }

  async putView(row: ViewRow, options?: CallOptions): Promise<void> {
    return this.putViews([row], options)
  }

  async putViews(rows: readonly ViewRow[], options?: CallOptions): Promise<void> {
    options?.signal?.throwIfAborted()
    // Every row is checked and copied before the first write, so a bad one writes nothing.
    const stored = rows.map((row) => {
      checkViewRow(row)

      return clone({
        ...row,
        sk: resolveViewKey(row.pk, row.sk),
        ttl: viewTtl(row),
      })
    })

    for (const row of stored) {
      const partition = this.#rows.get(row.pk) ?? new Map<string, ViewRow>()
      partition.set(row.sk, row)
      this.#rows.set(row.pk, partition)
    }
  }

  async deleteView(pk: string, sk: string, options?: CallOptions): Promise<void> {
    options?.signal?.throwIfAborted()
    const key = resolveViewKey(pk, sk)
    const partition = this.#rows.get(pk)
    partition?.delete(key)
    if (partition?.size === 0) {
      this.#rows.delete(pk)
    }
  }

  async batchGetViews(pks: readonly string[], options?: CallOptions): Promise<ViewRow[]> {
    return this.batchGetViewsAt(pks, '', options)
  }

  async batchGetViewsAt(
    pks: readonly string[],
    sk: string,
    options?: CallOptions,
  ): Promise<ViewRow[]> {
    options?.signal?.throwIfAborted()
    const keys = batchKeys(pks, sk)
    const rows: ViewRow[] = []
    for (const pk of keys.pks) {
      const row = this.#rows.get(pk)?.get(keys.sk)
      if (row !== undefined) {
        rows.push(clone(row))
      }
    }

    return rows
  }

  async listViewsByPk(pk: string, options?: CallOptions): Promise<ViewRow[]> {
    return allPages((cursor) => this.listViewsByPkPaged(pk, {...options, cursor}))
  }

  async listViewsByPkPaged(pk: string, options: PageOptions = {}): Promise<ViewPage> {
    options.signal?.throwIfAborted()
    nulFree(pk, 'pk')
    const limit = pageLimit(options.limit)
    const after = cursorKey(options.cursor, 'pk', pk)?.get('sk')

    const rows = sorted(
      [...(this.#rows.get(pk)?.values() ?? [])].filter(
        (row) => after === undefined || compareUtf8(row.sk, after) > 0,
      ),
    )

    return page(rows, limit, (row) => [
      ['pk', row.pk],
      ['sk', row.sk],
    ])
  }

  async listViewsByEntityType(entityType: string, options?: CallOptions): Promise<ViewRow[]> {
    return allPages((cursor) => this.listViewsByEntityTypePaged(entityType, {...options, cursor}))
  }

  async listViewsByEntityTypePaged(
    entityType: string,
    options: PageOptions = {},
  ): Promise<ViewPage> {
    options.signal?.throwIfAborted()
    nulFree(entityType, 'entity type')
    const limit = pageLimit(options.limit)
    const after = cursorKey(options.cursor, 'entityType', entityType)

    // A cursor resumes after its (pk, sk), the order the rows list in.
    const resume = after === null ? null : {pk: after.get('pk') ?? '', sk: after.get('sk') ?? ''}
    const rows: ViewRow[] = []
    for (const partition of this.#rows.values()) {
      for (const row of partition.values()) {
        if (row.entityType === entityType && (resume === null || byKey(row, resume) > 0)) {
          rows.push(row)
        }
      }
    }
    sorted(rows)

    return page(rows, limit, (row) => [
      ['entityType', row.entityType],
      ['pk', row.pk],
      ['sk', row.sk],
    ])
  }
}

// Takes one page off the ordered rows. The cursor is the last row's key, and only when a row
// follows it.
function page(
  rows: ViewRow[],
  limit: number | undefined,
  key: (row: ViewRow) => [string, string][],
): ViewPage {
  if (limit === undefined || rows.length <= limit) {
    return {views: rows.map(clone), nextCursor: null}
  }

  const views = rows.slice(0, limit)

  return {
    views: views.map(clone),
    nextCursor: encodeViewCursor(new Map(key(views.at(-1)!))),
  }
}

// Sorts rows in place by pk bytes, then sk bytes, and returns them.
function sorted(rows: ViewRow[]): ViewRow[] {
  // oxlint-disable-next-line unicorn/no-array-sort -- the array is this call's own copy
  return rows.sort(byKey)
}

function byKey(a: {pk: string; sk: string}, b: {pk: string; sk: string}): number {
  return compareUtf8(a.pk, b.pk) || compareUtf8(a.sk, b.sk)
}

// A copy that shares no bytes with the caller's. Not slice(): a Node Buffer's slice is a view of
// the same memory.
function clone(row: ViewRow): ViewRow {
  return {
    pk: row.pk,
    sk: row.sk,
    entityId: row.entityId,
    entityType: row.entityType,
    payload: Uint8Array.from(row.payload),
    ttl: row.ttl ?? 0,
  }
}
