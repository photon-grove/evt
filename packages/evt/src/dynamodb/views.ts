// The per-entity view projector: one row per entity in a views table, its state as the aggregate's
// codec encodes it, put in the same transaction as the commit's events (storage format, "Views").
// Commit-wide projectors, which own their keys, supersede it in Go and Rust, but a TypeScript
// writer of an entity type that a Go service projects this way must write the row too.

import type {Projection, Projector} from '../aggregate.js'
import {utf8Length} from '../utf8.js'
import type {TransactItem} from './client.js'
import {itemSize, viewItem} from './item.js'

/** The backend name of the projector groups the DynamoDB repository writes. */
export const DYNAMODB_BACKEND = 'dynamodb'

/**
 * A projector that puts the entity's view row in `viewsTable` with every commit:
 * `pk = {entityType}#{entityId}`, `sk = "VIEW"`, and the encoded state as its payload, as Go's
 * `dynamo.NewViewProjector` writes it. The put is unconditional, so the row is the state after the
 * entity's latest commit.
 */
export function viewProjector<S>(viewsTable: string): Projector<S> {
  if (viewsTable === '') {
    throw new RangeError('a view projector needs a views table')
  }

  return {
    project({entityType, entityId, encodedState}: Projection<S>) {
      if (entityId === '') {
        return null
      }

      const put: TransactItem = {
        Put: {TableName: viewsTable, Item: viewItem(entityType, entityId, encodedState)},
      }

      return {backend: DYNAMODB_BACKEND, operations: [put], sizes: [transactItemSize(put)]}
    },
  }
}

/**
 * An action's size in the commit budget: a put counts its item, and an update, delete or condition
 * check counts its key plus its expression strings, name placeholders (the placeholder and the
 * name) and attribute values.
 */
export function transactItemSize(action: TransactItem): number {
  if (action.Put) {
    return itemSize(action.Put.Item)
  }

  const body = action.Update ?? action.Delete ?? action.ConditionCheck
  if (!body) {
    return 0
  }

  let size = itemSize(body.Key) + utf8Length(body.ConditionExpression ?? '')
  if (action.Update) {
    size += utf8Length(action.Update.UpdateExpression)
  }
  for (const [placeholder, name] of Object.entries(body.ExpressionAttributeNames ?? {})) {
    size += utf8Length(placeholder) + utf8Length(name)
  }
  for (const [placeholder, value] of Object.entries(body.ExpressionAttributeValues ?? {})) {
    size += utf8Length(placeholder) + itemSize({'': value})
  }

  return size
}
