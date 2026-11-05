// The PostgreSQL row of a command outcome (storage format, "PostgreSQL"), in the command-outcomes
// table the events table names: `evt_events` yields `evt_events__evt_command_outcomes`. The JSON
// columns are jsonb, which reorders keys and adds whitespace, so a reader parses them rather than
// comparing their text.

import {invalid} from '../fields.js'
import {
  type Outcome,
  decodeEventIds,
  decodeIdentity,
  decodeRejection,
  encodeEventIds,
  encodeRejection,
  encodeTargets,
  validateStoredOutcome,
} from '../outcome.js'
import {type PgTables, outcomesTable} from './schema.js'

/** One row of the command-outcomes table, each column as the repository sends and reads it. */
export interface OutcomeRow {
  readonly command_id: string
  readonly status: string
  readonly digest: string
  /** `bytea`, null when the command returned no response. */
  readonly response: Uint8Array | null
  /** `jsonb`, as JSON text: the event IDs, `[]` when none. */
  readonly events: string
  /** `jsonb`, as JSON text, only when rejected. */
  readonly rejection: string | null
  /** Only when rejected. */
  readonly code: string | null
  /** The recorded identity's command type, null on a row that records none. */
  readonly command_type: string | null
  /** `jsonb`, as JSON text: the recorded identity's targets, null on a row that records none. */
  readonly targets: string | null
  /** The recorded identity's principal, null when the command had none. */
  readonly principal: string | null
}

/** The `INSERT` of an outcome row, which a commit runs last. A `23505` means another got there. */
export function insertOutcomeSql(tables: PgTables): string {
  return (
    `INSERT INTO ${outcomesTable(tables)} (command_id, status, digest, response, events, ` +
    'rejection, code, command_type, targets, principal) VALUES ($1, $2, $3, ' +
    "decode($4, 'hex'), $5::text::jsonb, $6::text::jsonb, $7, $8, $9::text::jsonb, $10)"
  )
}

/** The `SELECT` of an outcome row by command ID (`$1`), every column as text. */
export function selectOutcomeSql(tables: PgTables): string {
  return (
    "SELECT command_id, status, digest, encode(response, 'hex') AS response, " +
    'events::text AS events, rejection::text AS rejection, code, command_type, ' +
    `targets::text AS targets, principal FROM ${outcomesTable(tables)} WHERE command_id = $1`
  )
}

/** Encodes an outcome as its row. An empty response is stored as NULL, and no events as `[]`. */
export function outcomeRow(outcome: Outcome): OutcomeRow {
  const {identity, rejection} = outcome

  return {
    command_id: outcome.commandId,
    status: outcome.status,
    digest: outcome.digest,
    response: outcome.response !== null && outcome.response.length > 0 ? outcome.response : null,
    events: encodeEventIds(outcome.eventIds),
    rejection: rejection === null ? null : encodeRejection(rejection),
    code: rejection === null ? null : rejection.code,
    command_type: identity === null ? null : identity.commandType,
    targets: identity === null ? null : encodeTargets(identity),
    principal: identity?.principal ?? null,
  }
}

/**
 * Decodes an outcome row. A malformed one (an unknown status, a missing digest, a rejection or code
 * that disagrees with the status, a bad event ID, or a broken recorded identity) is
 * `invalidRecord`, never replayed as a result.
 */
export function outcomeFromRow(row: OutcomeRow): Outcome {
  const {status} = row
  if (status !== 'accepted' && status !== 'rejected') {
    throw invalid(`unknown outcome status ${JSON.stringify(status)}`)
  }

  const rejection = row.rejection === null ? null : decodeRejection(row.rejection)
  const outcome: Outcome = {
    commandId: row.command_id,
    status,
    digest: row.digest,
    response: row.response !== null && row.response.length > 0 ? row.response : null,
    eventIds: decodeEventIds(row.events),
    rejection,
    identity: null,
  }
  validateStoredOutcome(outcome)

  // The code column exists exactly with a rejection, and repeats its code.
  if (rejection === null && row.code !== null) {
    throw invalid(
      `stored outcome for command ${row.command_id} is corrupt: an accepted outcome carries a code`,
    )
  }
  if (rejection !== null && row.code !== rejection.code) {
    throw invalid(
      `stored outcome for command ${row.command_id} is corrupt: the code column disagrees with the rejection`,
    )
  }

  const identity = decodeIdentity(
    row.command_id,
    row.command_type ?? undefined,
    row.targets ?? undefined,
    row.principal ?? undefined,
  )

  return {...outcome, identity}
}
