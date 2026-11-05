// The DynamoDB item of a command outcome (storage format, "Command outcomes"):
//
// | Attribute     | Type | Outcome item (`pk = command-outcome#<id>`, `sk = 1`)            |
// | ------------- | ---- | --------------------------------------------------------------- |
// | `status`      | S    | `accepted` or `rejected`                                        |
// | `digest`      | S    | the identity digest                                             |
// | `response`    | S    | padded base64 of the response bytes, absent when none           |
// | `events`      | S    | JSON array of the event IDs the commit wrote, absent when none  |
// | `rejection`   | S    | the stored rejection, only when rejected                        |
// | `code`        | S    | the rejection's code, only when rejected                        |
// | `commandType` | S    | the recorded identity's command type                            |
// | `targets`     | S    | JSON array of its target stream IDs, exactly with `commandType` |
// | `principal`   | S    | its principal, only when the command had one                    |

import {base64Decode, base64Encode} from '../base64.js'
import {invalid} from '../fields.js'
import {
  COMMAND_OUTCOME_PREFIX,
  type Outcome,
  type OutcomeStatus,
  decodeEventIds,
  decodeIdentity,
  decodeRejection,
  encodeEventIds,
  encodeRejection,
  encodeTargets,
  validateStoredOutcome,
} from '../outcome.js'
import {type Item, itemSize, optionalString, requiredString} from './item.js'

/** The sort key of a command-outcome item. */
export const OUTCOME_SORT_KEY = 1n

/** The condition of the outcome put: no outcome is recorded for the command ID yet. */
export const OUTCOME_CONDITION = 'attribute_not_exists(pk)'

/** The partition key of a command's outcome row: `command-outcome#<id>`. */
export function outcomeKey(commandId: string): string {
  return `${COMMAND_OUTCOME_PREFIX}${commandId}`
}

/**
 * Encodes a command outcome as its item, with the attributes Go and Rust write: the response when
 * it has bytes, the event IDs when the commit wrote any, the rejection and its code when
 * rejected, and the recorded identity when the outcome has one.
 */
export function outcomeItem(outcome: Outcome): Item {
  const item: Item = {
    pk: {S: outcomeKey(outcome.commandId)},
    sk: {N: OUTCOME_SORT_KEY.toString()},
    status: {S: outcome.status},
    digest: {S: outcome.digest},
  }
  if (outcome.response !== null && outcome.response.length > 0) {
    item['response'] = {S: base64Encode(outcome.response)}
  }
  if (outcome.eventIds.length > 0) {
    item['events'] = {S: encodeEventIds(outcome.eventIds)}
  }
  if (outcome.rejection !== null) {
    item['rejection'] = {S: encodeRejection(outcome.rejection)}
    item['code'] = {S: outcome.rejection.code}
  }

  const {identity} = outcome
  if (identity !== null) {
    item['commandType'] = {S: identity.commandType}
    item['targets'] = {S: encodeTargets(identity)}
    if (identity.principal !== null) {
      item['principal'] = {S: identity.principal}
    }
  }

  return item
}

/** The outcome item's size, as the commit budget counts it. */
export function outcomeItemSize(outcome: Outcome): number {
  return itemSize(outcomeItem(outcome))
}

/**
 * Decodes a command-outcome item, the command ID from its key. A malformed row (an unknown status,
 * a missing digest, a rejection that disagrees with the status or the `code` attribute, a bad event
 * ID or response, or a broken recorded identity) is `invalidRecord`, never replayed as a result.
 */
export function outcomeFromItem(item: Item): Outcome {
  const pk = requiredString(item, 'pk')
  if (!pk.startsWith(COMMAND_OUTCOME_PREFIX)) {
    throw invalid(`item ${pk} is not a command outcome`)
  }
  const commandId = pk.slice(COMMAND_OUTCOME_PREFIX.length)

  const status = requiredString(item, 'status')
  if (status !== 'accepted' && status !== 'rejected') {
    throw invalid(`unknown outcome status ${JSON.stringify(status)}`)
  }

  const responseText = nonEmpty(optionalString(item, 'response'))
  let response: Uint8Array | null = null
  if (responseText !== undefined) {
    try {
      response = base64Decode(responseText)
    } catch (error) {
      throw invalid(`decode outcome response: ${error instanceof Error ? error.message : error}`)
    }
  }

  const events = nonEmpty(optionalString(item, 'events'))
  const rejectionText = nonEmpty(optionalString(item, 'rejection'))
  const rejection = rejectionText === undefined ? null : decodeRejection(rejectionText)
  if ((status === 'rejected') !== (rejection !== null)) {
    throw invalid(`outcome ${commandId} is ${status} but its rejection attribute disagrees`)
  }

  const outcome: Outcome = {
    commandId,
    status: status satisfies OutcomeStatus,
    digest: optionalString(item, 'digest') ?? '',
    response,
    eventIds: events === undefined ? [] : decodeEventIds(events),
    rejection,
    identity: null,
  }
  validateStoredOutcome(outcome)

  // The code attribute exists exactly with a rejection, and repeats its code.
  const code = optionalString(item, 'code')
  if (rejection === null && code !== undefined) {
    throw invalid(
      `stored outcome for command ${commandId} is corrupt: an accepted outcome carries a code attribute`,
    )
  }
  if (rejection !== null && (code ?? '') !== rejection.code) {
    throw invalid(
      `stored outcome for command ${commandId} is corrupt: the code attribute disagrees with the rejection`,
    )
  }

  const identity = decodeIdentity(
    commandId,
    optionalString(item, 'commandType'),
    optionalString(item, 'targets'),
    optionalString(item, 'principal'),
  )

  return {...outcome, identity}
}

function nonEmpty(text: string | undefined): string | undefined {
  return text === '' ? undefined : text
}
