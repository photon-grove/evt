// Durable command outcomes (ADR 0004, C3 and C4): the row a store with outcomes records once per
// command, in its final state, on the reserved `command-outcome#<id>` stream inside the commit
// transaction, and the rules every backend applies to it alike (storage format, "Command
// outcomes", "Rejections"). This module holds the row and its validation, the stored rejection,
// the recorded identity, and the identity digest. The backends' codecs are in `dynamodb/outcome.ts`
// and `postgres/repository.ts`.

import type {Rejection} from './aggregate.js'
import {CommandIdTooLongError, EvtError, OutcomeTooLargeError} from './errors.js'
import type {SerializedEvent} from './event.js'
import {DOCUMENT, invalid} from './fields.js'
import {type JsonValue, RawJson, parseJson, stringifyJson} from './json.js'
import type {Metadata} from './metadata.js'
import {MAX_OUTCOME_BYTES, canonicalNumbers} from './numbers.js'
import {compareUtf8, isHighSurrogate, isLowSurrogate, utf8Encode, utf8Length} from './utf8.js'

export {MAX_OUTCOME_BYTES} from './numbers.js'

/** The most UTF-8 bytes a command ID may take. */
export const MAX_COMMAND_ID_BYTES = 512

/** The partition-key prefix of the reserved command-outcome rows. */
export const COMMAND_OUTCOME_PREFIX = 'command-outcome#'

/**
 * A command's identity, the caller's intent fixed before any state is read: its type, the streams
 * the request names (`targets`), and its payload's canonical JSON. The command ID and the
 * principal come from the metadata. The streams a command ends up reading or writing aren't part
 * of it, since they may depend on state and change between attempts, so everything that
 * distinguishes a request must be in the targets or the payload.
 */
export interface CommandIdentity {
  readonly commandType: string
  /** The streams the request names. None for a command that names no stream. */
  readonly targets?: readonly string[] | undefined
  /** The payload's canonical JSON, empty or absent when it has none. */
  readonly payload?: Uint8Array | null | undefined
}

/**
 * What a command on a store with outcomes did: the recorded outcome, and what this call wrote.
 */
export interface CommandOutcome {
  /** The outcome this call recorded, or the stored one it replayed. */
  readonly outcome: Outcome
  /**
   * The events this call committed, by entity ID bytes then sequence. Empty when the outcome was
   * replayed, and for a no-op or a rejection that carries no events.
   */
  readonly events: readonly SerializedEvent[]
  /**
   * The outcome was read from a stored row (a repeated command, a lost race the winner's row
   * settled, or an ambiguous write's recovery), rather than produced by this call.
   */
  readonly replayed: boolean
}

/** The engine's rejection codes, which a store may rename (Go and Rust pin them). */
export const REJECTION_CODES = Object.freeze({
  notFound: 'not-found',
  badCommand: 'bad-command',
  staleFence: 'stale-fence',
  staleVersion: 'stale-version',
})

// The tag that pins the digest's canonical encoding.
const DIGEST_TAG = 'evt-cmd-digest-v1'

/** Whether a command was accepted or rejected, final when its outcome is written. */
export type OutcomeStatus = 'accepted' | 'rejected'

/**
 * The command identity an outcome row records beside its digest (storage format, "Recorded
 * identity"): the command type, the target streams deduplicated and sorted by UTF-8 bytes as the
 * digest takes them, and the principal, null when the command had none. It's descriptive: the
 * digest stays the idempotency key.
 */
export interface RecordedIdentity {
  readonly commandType: string
  readonly targets: readonly string[]
  readonly principal: string | null
}

/**
 * The durable record of one command: the row written once, in its final state, on the reserved
 * `command-outcome#<id>` stream.
 *
 * A rejection decoded from storage carries its `details` as a `Map` of {@link JsonValue}s, with
 * each number's stored text in a `JsonNumber`, so nothing is rounded; `null` when it has none.
 */
export interface Outcome {
  /** The command ID the row is keyed by. */
  readonly commandId: string
  readonly status: OutcomeStatus
  /** The identity digest the command was recorded under. */
  readonly digest: string
  /** The stored response bytes, null when the command returned none. */
  readonly response: Uint8Array | null
  /**
   * The events the commit wrote, by stream ID bytes, then sequence. Empty for an accepted no-op
   * and a rejection without events. A commit fills them from its own events.
   */
  readonly eventIds: readonly string[]
  /** The rejection, exactly when the status is `rejected`. */
  readonly rejection: Rejection | null
  /** The identity the digest was computed from, null on a row written before rows recorded it. */
  readonly identity: RecordedIdentity | null
}

/** What the identity digest is computed from (storage format, "Identity digest"). */
export interface DigestInput {
  readonly commandType: string
  /** The streams the request names, before any state is read. Sorted and deduplicated here. */
  readonly targets: readonly string[]
  /** The command's canonical payload JSON, empty for a command with none. */
  readonly payload: Uint8Array
  /** The metadata's principal, `""` when unset. */
  readonly principal: string
}

/** A stream whose client-expected head went stale, for {@link staleVersionRejection}. */
export interface StaleVersion {
  readonly entityId: string
  readonly expected: bigint
  readonly actual: bigint
}

/**
 * The engine's rejection for a caller-stamped fence that went stale before the commit: the engine
 * can't know the caller's decision still holds, so it doesn't retry.
 */
export function staleFenceRejection(): Rejection {
  return {
    code: REJECTION_CODES.staleFence,
    message: 'a caller-stamped fence went stale',
    details: null,
  }
}

/**
 * The engine's rejection for fence-aware retry that ran out of attempts. The attempt count is an
 * integer, so every language stores the same bytes.
 */
export function retriesExhaustedRejection(attempts: number): Rejection {
  return {
    code: REJECTION_CODES.staleFence,
    message: 'the state this command was decided on kept changing; retries were exhausted',
    details: {attempts},
  }
}

/** The engine's rejection for client-expected heads that went stale, sorted by entity ID. */
export function staleVersionRejection(stale: readonly StaleVersion[]): Rejection {
  const sorted = [...stale]
  sorted.sort((a, b) => compareUtf8(a.entityId, b.entityId))

  return {
    code: REJECTION_CODES.staleVersion,
    message: 'a client-expected stream version is stale',
    details: {
      streams: sorted.map(({entityId, expected, actual}) => ({actual, entityId, expected})),
    },
  }
}

/**
 * Encodes a rejection as its stored JSON, byte for byte as Go and Rust write it: keys `code`,
 * `message`, `details`, with `details` `null` when absent and every object in it written with its
 * keys sorted by UTF-8 bytes, as a Go map and a Rust `Value` are. Numbers take their canonical
 * text. The checks run in the order the storage format pins, each an `invalidArgument` but the
 * last: text that isn't well-formed UTF-16 in the code or message, U+0000 anywhere, details that
 * aren't an object, a number PostgreSQL can't store, then the size bound
 * ({@link OutcomeTooLargeError}).
 */
export function encodeRejection(rejection: Rejection): string {
  checkWellFormed('a rejection code', rejection.code)
  checkWellFormed('a rejection message', rejection.message)

  const details = rejection.details ?? null
  let encodedDetails: string
  let parsed: JsonValue
  let nul: boolean
  try {
    encodedDetails = details === null ? 'null' : stringifyJson(asGoMaps(details))
    parsed = parseJson(encodedDetails)
    nul = hasNul(parsed)
  } catch (error) {
    // The walks below recurse, so details nested past the call stack are refused, not thrown raw.
    if (error instanceof RangeError) {
      throw new EvtError('invalidArgument', 'rejection details are nested too deeply', {
        cause: error,
      })
    }

    throw error
  }
  if (rejection.code.includes('\0') || rejection.message.includes('\0') || nul) {
    throw new EvtError(
      'invalidArgument',
      'a rejection must not contain U+0000 in its code, message, or details',
    )
  }
  if (parsed !== null && !(parsed instanceof Map)) {
    throw new EvtError('invalidArgument', 'rejection details must be a JSON object or absent')
  }

  const encoded = stringifyJson({
    code: rejection.code,
    message: rejection.message,
    details: new RawJson(encodedDetails),
  })

  return canonicalNumbers(encoded)
}

/**
 * Parses the stored JSON form of a rejection: one complete JSON document, with `details` an object
 * or `null`. Anything else is a corrupt row (`invalidRecord`).
 */
export function decodeRejection(text: string): Rejection {
  let value: JsonValue
  try {
    value = parseJson(text, DOCUMENT)
  } catch (error) {
    throw invalid(`decode rejection: ${message(error)}`)
  }
  if (!(value instanceof Map)) {
    throw invalid('decode rejection: not a JSON object')
  }
  if (!value.has('code')) {
    throw invalid('decode rejection: missing field code')
  }

  const details = value.get('details') ?? null
  if (details !== null && !(details instanceof Map)) {
    throw invalid('decode rejection: details must be a JSON object or null')
  }

  return {
    code: stringOrEmpty(value, 'code'),
    message: stringOrEmpty(value, 'message'),
    details,
  }
}

/**
 * The rejection as it reads back from storage: encoded, then decoded, its numbers canonical. A
 * first execution returns this form, so it equals a replay on every backend.
 */
export function canonicalRejection(rejection: Rejection): Rejection {
  return decodeRejection(encodeRejection(rejection))
}

/**
 * Checks a command ID before any read or write: required, free of U+0000 and well-formed UTF-16
 * (`invalidArgument`), and at most {@link MAX_COMMAND_ID_BYTES} UTF-8 bytes
 * ({@link CommandIdTooLongError}).
 */
export function checkCommandId(commandId: string): void {
  if (commandId === '') {
    throw new EvtError(
      'invalidArgument',
      'a command ID is required when command outcomes are enabled',
    )
  }
  checkText('a command ID', commandId)

  const length = utf8Length(commandId)
  if (length > MAX_COMMAND_ID_BYTES) {
    throw new CommandIdTooLongError(length)
  }
}

/**
 * Checks every caller-supplied string of the metadata that's persisted on a command's events: the
 * command, causation and principal IDs, the trace carrier, the origin, the address, the region,
 * the timestamp, the extension keys and values, and the commit ID. None may hold U+0000, which
 * PostgreSQL can't store, or a lone surrogate, which isn't UTF-8 (`invalidArgument`).
 */
export function checkMetadata(metadata: Metadata): void {
  const optional = [
    ['commandId', metadata.commandId],
    ['causationId', metadata.causationId],
    ['principalId', metadata.principalId],
    ['address', metadata.address],
  ] as const
  for (const [what, value] of optional) {
    if (value !== null) {
      checkText(what, value)
    }
  }

  checkText('region', metadata.region)
  checkText('timestamp', metadata.timestamp)
  checkText('commitId', metadata.commitId)
  if (metadata.origin !== null) {
    checkText('origin.source', metadata.origin.source)
    checkText('origin.endpoint', metadata.origin.endpoint)
  }
  for (const [key, value] of metadata.trace ?? []) {
    checkText('trace', key)
    checkText('trace', value)
  }
  for (const [key, value] of metadata.extensions) {
    checkText('extensions', key)
    checkText('extensions', value)
  }
}

/**
 * Checks the identifiers and metadata of a commit that records an outcome: every event's ID, type,
 * entity ID and entity type and its metadata, and every other entity ID and type it names.
 */
export function checkCommitText(
  events: Iterable<SerializedEvent>,
  entityTypes: Iterable<string>,
  entityIds: Iterable<string>,
): void {
  for (const event of events) {
    checkText('event.id', event.id)
    checkText('event.type', event.type)
    checkText('event.entityId', event.entityId)
    checkText('event.entityType', event.entityType)
    checkMetadata(event.metadata)
  }
  for (const entityType of entityTypes) {
    checkText('entityType', entityType)
  }
  for (const entityId of entityIds) {
    checkText('entityId', entityId)
  }
}

/**
 * Validates an outcome before any write, in the order the storage format pins: the command ID, the
 * digest, the status and its rejection, then the response's bound, the rejection's (by encoding
 * it), and the recorded identity's.
 */
export function checkOutcome(outcome: Outcome): void {
  checkCommandId(outcome.commandId)
  if (outcome.digest === '') {
    throw new EvtError('invalidArgument', 'an outcome needs an identity digest')
  }
  checkText('an identity digest', outcome.digest)

  if (outcome.status !== 'accepted' && outcome.status !== 'rejected') {
    throw new EvtError('invalidArgument', `unknown outcome status ${String(outcome.status)}`)
  }
  if (outcome.status === 'accepted' && outcome.rejection !== null) {
    throw new EvtError('invalidArgument', 'an accepted outcome carries no rejection')
  }
  if (
    outcome.status === 'rejected' &&
    (outcome.rejection === null || outcome.rejection.code === '')
  ) {
    throw new EvtError('invalidArgument', 'a rejected outcome needs a rejection with a code')
  }

  const response = outcome.response?.length ?? 0
  if (response > MAX_OUTCOME_BYTES) {
    throw new OutcomeTooLargeError('response', response)
  }
  if (outcome.rejection !== null) {
    encodeRejection(outcome.rejection)
  }
  if (outcome.identity !== null) {
    checkText('a command type', outcome.identity.commandType)
    for (const target of outcome.identity.targets) {
      checkText('a target stream ID', target)
    }
    if (outcome.identity.principal !== null) {
      checkText('a principal', outcome.identity.principal)
    }

    const size = identitySize(outcome.identity)
    if (size > MAX_OUTCOME_BYTES) {
      throw new OutcomeTooLargeError('identity', size)
    }
  }
}

/**
 * Checks an outcome decoded from storage: a digest, a rejection with a code exactly when the
 * status is rejected, and event IDs of the form `{entityId}:{sequence}` with a positive, canonical
 * sequence. A malformed row is `invalidRecord`, never replayed as a result.
 */
export function validateStoredOutcome(outcome: Outcome): void {
  const corrupt = (reason: string): EvtError =>
    invalid(`stored outcome for command ${outcome.commandId} is corrupt: ${reason}`)

  if (outcome.digest === '') {
    throw corrupt('the digest is missing')
  }
  if (outcome.status === 'accepted' && outcome.rejection !== null) {
    throw corrupt('an accepted outcome carries a rejection')
  }
  if (
    outcome.status === 'rejected' &&
    (outcome.rejection === null || outcome.rejection.code === '')
  ) {
    throw corrupt('a rejected outcome has no rejection code')
  }

  for (const id of outcome.eventIds) {
    const at = id.lastIndexOf(':')
    const sequence = at === -1 ? '' : id.slice(at + 1)
    const canonical = at > 0 && /^[1-9]\d*$/.test(sequence) && BigInt(sequence) <= 2n ** 63n - 1n
    if (!canonical) {
      throw corrupt(
        `event ID ${JSON.stringify(id)} is not {entityId}:{sequence} with a positive canonical sequence`,
      )
    }
  }
}

/** The outcome as a commit stores it: its response normalized and the commit's event IDs. */
export function storedOutcome(outcome: Outcome, eventIds: readonly string[]): Outcome {
  const response =
    outcome.response !== null && outcome.response.length > 0 ? outcome.response : null

  return {...outcome, response, eventIds: [...eventIds]}
}

/** Encodes event IDs as the stored JSON array. */
export function encodeEventIds(ids: readonly string[]): string {
  return stringifyJson([...ids])
}

/** Parses the stored JSON array of event IDs. JSON `null` is none. */
export function decodeEventIds(text: string): string[] {
  let value: JsonValue
  try {
    value = parseJson(text, DOCUMENT)
  } catch (error) {
    throw invalid(`decode event IDs: ${message(error)}`)
  }
  if (value === null) {
    return []
  }
  if (!Array.isArray(value) || !value.every((id) => typeof id === 'string')) {
    throw invalid('decode event IDs: not a JSON array of strings')
  }

  return value as string[]
}

/** The identity an outcome records for a digest input. An empty principal is none. */
export function recordedIdentity(input: DigestInput): RecordedIdentity {
  return {
    commandType: input.commandType,
    targets: sortedTargets(input.targets),
    principal: input.principal === '' ? null : input.principal,
  }
}

/** The stored JSON array of an identity's target stream IDs: `[]` when there are none. */
export function encodeTargets(identity: RecordedIdentity): string {
  return stringifyJson([...identity.targets])
}

/**
 * The bytes an identity's fields take, as the bound counts them: the UTF-8 bytes of the command
 * type, the principal, and the encoded targets, escapes included.
 */
export function identitySize(identity: RecordedIdentity): number {
  return (
    utf8Length(identity.commandType) +
    utf8Length(encodeTargets(identity)) +
    utf8Length(identity.principal ?? '')
  )
}

/**
 * Rebuilds a row's recorded identity from its stored fields, each undefined when absent. A row
 * without a command type records none (null), unless it has targets or a principal. A command type
 * without targets, or targets that aren't a JSON array of strings, is a corrupt row.
 */
export function decodeIdentity(
  commandId: string,
  commandType: string | undefined,
  targets: string | undefined,
  principal: string | undefined,
): RecordedIdentity | null {
  const corrupt = (reason: string): EvtError =>
    invalid(`stored outcome for command ${commandId} is corrupt: ${reason}`)

  if (commandType === undefined) {
    if (targets !== undefined || principal !== undefined) {
      throw corrupt('the recorded identity has targets or a principal but no command type')
    }

    return null
  }
  if (targets === undefined) {
    throw corrupt('the recorded identity has a command type but no targets')
  }

  let decoded: JsonValue
  try {
    decoded = parseJson(targets, DOCUMENT)
  } catch {
    throw corrupt(
      `the recorded targets are not a JSON array of strings: ${JSON.stringify(targets)}`,
    )
  }
  if (!Array.isArray(decoded) || !decoded.every((target) => typeof target === 'string')) {
    throw corrupt(
      `the recorded targets are not a JSON array of strings: ${JSON.stringify(targets)}`,
    )
  }

  return {commandType, targets: decoded as string[], principal: principal ?? null}
}

/**
 * The canonical bytes the identity digest hashes: the tag and a NUL, then the command type, the
 * target count and each target (sorted by UTF-8 bytes and deduplicated), the payload and the
 * principal, each length-prefixed with a little-endian uint32.
 */
export function canonicalDigestBytes(input: DigestInput): Uint8Array<ArrayBuffer> {
  const parts: Uint8Array[] = [utf8Encode(DIGEST_TAG), new Uint8Array([0])]
  const prefixed = (bytes: Uint8Array): void => {
    parts.push(uint32(bytes.length), bytes)
  }

  prefixed(utf8Encode(input.commandType))
  const targets = sortedTargets(input.targets)
  parts.push(uint32(targets.length))
  for (const target of targets) {
    prefixed(utf8Encode(target))
  }
  prefixed(input.payload)
  prefixed(utf8Encode(input.principal))

  const out = new Uint8Array(parts.reduce((size, part) => size + part.length, 0))
  let offset = 0
  for (const part of parts) {
    out.set(part, offset)
    offset += part.length
  }

  return out
}

/** The default identity digest: the lowercase hex SHA-256 of {@link canonicalDigestBytes}. */
export async function defaultDigest(input: DigestInput): Promise<string> {
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', canonicalDigestBytes(input)))
  let hex = ''
  for (const byte of digest) {
    hex += byte.toString(16).padStart(2, '0')
  }

  return hex
}

// Refuses a string PostgreSQL can't store (U+0000) or that isn't UTF-8 (a lone surrogate), so a
// command has the same result on every backend.
/** Refuses text no backend can store alike: U+0000, or a lone surrogate. */
export function checkText(what: string, value: string): void {
  if (value.includes('\0')) {
    throw new EvtError('invalidArgument', `${what} must not contain U+0000`)
  }
  checkWellFormed(what, value)
}

function checkWellFormed(what: string, value: string): void {
  for (let i = 0; i < value.length; i++) {
    const unit = value.charCodeAt(i)
    if (isHighSurrogate(unit) && isLowSurrogate(value.charCodeAt(i + 1))) {
      i++
    } else if (isHighSurrogate(unit) || isLowSurrogate(unit)) {
      throw new EvtError(
        'invalidArgument',
        `${what} must be valid UTF-8: it holds a lone surrogate`,
      )
    }
  }
}

function sortedTargets(targets: readonly string[]): string[] {
  const sorted = [...new Set(targets)]
  sorted.sort(compareUtf8)

  return sorted
}

function uint32(value: number): Uint8Array {
  const bytes = new Uint8Array(4)
  new DataView(bytes.buffer).setUint32(0, value, true)

  return bytes
}

// Rewrites every plain object of a details value as a Map, so the writer sorts its keys the way a
// Go map and a Rust Value are written. Arrays are walked; every other value is kept.
function asGoMaps(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(asGoMaps)
  }
  if (value instanceof Map) {
    return new Map([...value].map(([key, entry]) => [key, asGoMaps(entry)]))
  }
  if (isPlainObject(value)) {
    // An undefined property is skipped, as the writer skips it on a plain object.
    const entries = Object.entries(value).filter(([, entry]) => entry !== undefined)

    return new Map(entries.map(([key, entry]) => [key, asGoMaps(entry)]))
  }

  return value
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== 'object' || value === null) {
    return false
  }

  const prototype: unknown = Object.getPrototypeOf(value)

  return prototype === Object.prototype || prototype === null
}

// Whether a parsed JSON value holds U+0000 in any string or object key, at any depth.
function hasNul(value: JsonValue): boolean {
  if (typeof value === 'string') {
    return value.includes('\0')
  }
  if (Array.isArray(value)) {
    return value.some(hasNul)
  }
  if (value instanceof Map) {
    return [...value].some(([key, entry]) => key.includes('\0') || hasNul(entry))
  }

  return false
}

// A string field that may be null (read as `""`); any other type is a corrupt row.
function stringOrEmpty(object: Map<string, JsonValue>, name: string): string {
  const value = object.get(name)
  if (value === undefined || value === null) {
    return ''
  }
  if (typeof value !== 'string') {
    throw invalid(`decode rejection: ${name} is not a string`)
  }

  return value
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
