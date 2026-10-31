// The DynamoDB actions on a stream's head item (ADR 0007), independent of any SDK: the one source
// of the head's expression strings, which the writer builds its actions from and the commit
// budget sizes. The fence and snapshot-guard forms join them with the DynamoDB backend.

import {safeIntegerArgument} from '../fields.js'
import {utf8Length} from '../utf8.js'
import {HEAD_SORT_KEY, itemSize} from './item.js'

/** The update expression of a head update on a stream without retention. */
export const HEAD_UPDATE = 'SET head = :last'

/** The update expression of a head update on a stream with retention, which raises `ttl` too. */
export const HEAD_UPDATE_RETAINED = 'SET head = :last, #ttl = :t'

/** The condition of the head update of a stream the commit creates (first sequence 1). */
export const HEAD_CREATE_CONDITION = 'attribute_not_exists(head)'

/** The condition of the head update of an append: the head is the sequence before the first. */
export const HEAD_APPEND_CONDITION = 'head = :prev'

/** {@link HEAD_APPEND_CONDITION} with retention: `ttl` is raised, never lowered. */
export const HEAD_APPEND_RETAINED_CONDITION =
  'head = :prev AND (attribute_not_exists(#ttl) OR #ttl <= :t)'

/** An `Update` or `ConditionCheck` on a stream's head item. */
export interface HeadAction {
  entityId: string
  /** The update expression, or null for a `ConditionCheck`. */
  updateExpression: string | null
  conditionExpression: string
  /** `ExpressionAttributeNames`: placeholder and attribute name. */
  names: [string, string][]
  /** `ExpressionAttributeValues`: placeholder and number. */
  values: [string, bigint][]
}

/**
 * The head update of a stream whose commit writes events `first` to `last`. A first sequence of 1
 * creates the head. `ttl` is the `:t` of the never-lower rule, given only for a stream with
 * retention.
 */
export function headUpdate(
  entityId: string,
  first: bigint,
  last: bigint,
  ttl?: number,
): HeadAction {
  const create = first <= 1n
  const values: [string, bigint][] = [[':last', last]]
  const names: [string, string][] = []

  if (!create) {
    values.push([':prev', first - 1n])
  }
  if (ttl !== undefined) {
    names.push(['#ttl', 'ttl'])
    values.push([':t', BigInt(safeIntegerArgument(ttl, 'ttl'))])
  }

  let conditionExpression = HEAD_CREATE_CONDITION
  if (!create) {
    conditionExpression =
      ttl === undefined ? HEAD_APPEND_CONDITION : HEAD_APPEND_RETAINED_CONDITION
  }

  return {
    entityId,
    updateExpression: ttl === undefined ? HEAD_UPDATE : HEAD_UPDATE_RETAINED,
    conditionExpression,
    names,
    values,
  }
}

/**
 * An action's size in the commit budget, by the rule for any update, delete or condition check:
 * its key attributes plus its expression strings, name placeholders (the placeholder and the
 * name), and attribute values (the placeholder and the number's text).
 */
export function headActionSize(action: HeadAction): number {
  const key = itemSize({pk: {S: action.entityId}, sk: {N: HEAD_SORT_KEY.toString()}})
  let size =
    key + utf8Length(action.updateExpression ?? '') + utf8Length(action.conditionExpression)

  for (const [placeholder, name] of action.names) {
    size += utf8Length(placeholder) + utf8Length(name)
  }
  for (const [placeholder, number] of action.values) {
    size += utf8Length(placeholder) + number.toString().length
  }

  return size
}
