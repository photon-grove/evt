// Running commands with durable outcomes (ADR 0004, C3 and C4; storage format, "Command outcomes"
// and "Ambiguous-write recovery"): the command a store or a transaction freezes before its first
// attempt, the outcome check every attempt starts with, how a failed commit that carried the
// outcome settles, and the retry budget of a single-aggregate command. The row itself is in
// outcome.ts, and the backends write it.

import type {Rejection} from './aggregate.js'
import {
  AmbiguousWriteError,
  EvtError,
  FenceConflictError,
  IdempotencyKeyReuseError,
  OutcomeConflictError,
  TransientError,
  isConflict,
  messageOf,
} from './errors.js'
import type {SerializedEvent} from './event.js'
import type {Metadata} from './metadata.js'
import {
  type CommandIdentity,
  type CommandOutcome,
  type DigestInput,
  type Outcome,
  type RecordedIdentity,
  canonicalRejection,
  checkCommandId,
  checkMetadata,
  checkText,
  defaultDigest,
  recordedIdentity,
  storedOutcome,
} from './outcome.js'
import {type CallOptions, type OutcomeRepository, checkNotReserved} from './repository.js'
import {
  DEFAULT_RETRY_POLICY,
  type RetryPolicy,
  checkRetryPolicy,
  retryDelay,
  sleep,
} from './retry.js'
import {copyMetadata} from './staging.js'

/** How long ambiguous-write recovery polls for the outcome row by default: 10 seconds. */
export const DEFAULT_RECOVERY_WAIT_MS = 10_000

// How long past the recovery window the last read may still take to answer, and the backoff
// between recovery reads: 50 ms, doubling to 1 s. Go and Rust use the same.
const RECOVERY_GRACE_MS = 1000
const RECOVERY_FIRST_DELAY_MS = 50
const RECOVERY_MAX_DELAY_MS = 1000

// The longest recovery window a timer can bound with its grace period: runtimes fire a timer past
// 2^31 - 1 ms at once.
const MAX_RECOVERY_WAIT_MS = 2 ** 31 - 1 - RECOVERY_GRACE_MS

/** How a store records command outcomes. Every field has a default. */
export interface OutcomeOptions {
  /**
   * Computes a command's identity digest, an opaque string the store compares for equality only.
   * {@link defaultDigest} by default. It must not return an empty string or one with U+0000.
   */
  digest?: ((input: DigestInput) => string | Promise<string>) | undefined
  /**
   * How long recovery polls for the outcome row of a commit whose result is unknown, in
   * milliseconds, 10 seconds by default. A row still absent then is an {@link AmbiguousWriteError}.
   */
  recoveryWaitMs?: number | undefined
  /**
   * Bounds the attempts of a single-aggregate command, {@link DEFAULT_RETRY_POLICY} by default.
   * When the last attempt conflicts, the store records a stale-fence rejection as the outcome.
   */
  retry?: RetryPolicy | undefined
  /**
   * The code of the stale-fence rejections the engine records: a failed caller-stamped fence and
   * exhausted retries. `stale-fence` by default; the message and details are unchanged.
   */
  staleFenceCode?: string | undefined
  /** Waits between attempts and between recovery reads. A timer by default; inject one in tests. */
  sleep?: ((ms: number, signal?: AbortSignal) => Promise<void>) | undefined
}

/** A store's outcome options with every default filled in. */
export class OutcomeConfig {
  readonly recoveryWaitMs: number
  readonly retry: RetryPolicy
  readonly staleFenceCode: string
  readonly sleep: (ms: number, signal?: AbortSignal) => Promise<void>
  readonly #digest: (input: DigestInput) => string | Promise<string>

  constructor(options: OutcomeOptions) {
    const wait = options.recoveryWaitMs ?? DEFAULT_RECOVERY_WAIT_MS
    if (!Number.isSafeInteger(wait) || wait < 0 || wait > MAX_RECOVERY_WAIT_MS) {
      throw new EvtError(
        'invalidArgument',
        'the recovery wait must be a whole number of milliseconds from 0 to ' +
          `${MAX_RECOVERY_WAIT_MS}: ${wait}`,
      )
    }
    const retry = options.retry ?? DEFAULT_RETRY_POLICY
    checkRetryPolicy(retry)

    this.recoveryWaitMs = wait
    this.retry = {...retry}
    this.staleFenceCode = options.staleFenceCode || 'stale-fence'
    this.sleep = options.sleep ?? sleep
    this.#digest = options.digest ?? defaultDigest
  }

  /** The engine's stale-fence rejection under the configured code. */
  staleFence(rejection: Rejection): Rejection {
    return {...rejection, code: this.staleFenceCode}
  }

  /** The identity digest, refused when it couldn't be stored or compared. */
  async digest(input: DigestInput): Promise<string> {
    checkText('the command type', input.commandType)
    checkText('the principal', input.principal)

    let digest: unknown
    try {
      digest = await this.#digest(input)
    } catch (error) {
      throw new EvtError('invalidArgument', `the command digester failed: ${messageOf(error)}`, {
        cause: error,
      })
    }
    if (typeof digest !== 'string' || digest === '') {
      throw new EvtError('invalidArgument', 'the command digester returned an empty digest')
    }
    checkText('the command digest', digest)

    return digest
  }
}

/**
 * A command whose ID, identity digest and metadata are validated and computed once, before its
 * first attempt, so every attempt, a client's retry and a recovery read compare equal.
 */
export interface PendingCommand {
  readonly commandId: string
  readonly digest: string
  /** The identity the outcome row records beside the digest. */
  readonly identity: RecordedIdentity
  /** The command's metadata, as its commit stamps it. */
  readonly metadata: Metadata
}

/**
 * Validates a command and computes its identity digest: its targets (no U+0000, no reserved
 * namespace), its command ID (present, at most 512 UTF-8 bytes), the metadata's strings, and the
 * digest. Nothing is read.
 */
export async function freezeCommand(
  config: OutcomeConfig,
  identity: CommandIdentity,
  metadata: Metadata,
): Promise<PendingCommand> {
  const targets = [...(identity.targets ?? [])]
  for (const target of targets) {
    checkText('a target stream ID', target)
    checkNotReserved(target)
  }
  const commandId = requireCommandId(metadata)

  const input: DigestInput = {
    commandType: identity.commandType,
    targets,
    payload: identity.payload ?? new Uint8Array(),
    principal: metadata.principalId ?? '',
  }

  return {
    commandId,
    digest: await config.digest(input),
    identity: recordedIdentity(input),
    metadata: copyMetadata(metadata),
  }
}

/** The command ID a command with outcomes must have, checked with the rest of its metadata. */
export function requireCommandId(metadata: Metadata): string {
  const commandId = metadata.commandId
  if (commandId === null || commandId === '') {
    throw new EvtError(
      'invalidArgument',
      'a command ID is required when command outcomes are enabled',
    )
  }
  checkCommandId(commandId)
  checkMetadata(metadata)

  return commandId
}

/** The command's accepted outcome, with its response and its identity recorded. */
export function acceptedOutcome(
  pending: PendingCommand,
  response: Uint8Array | null | undefined,
): Outcome {
  return {
    commandId: pending.commandId,
    status: 'accepted',
    digest: pending.digest,
    response: response === undefined || response === null ? null : new Uint8Array(response),
    eventIds: [],
    rejection: null,
    identity: pending.identity,
  }
}

/**
 * The command's rejected outcome. The rejection is canonicalized through its stored encoding, so
 * a first execution and a replay return the same rejection.
 */
export function rejectedOutcome(pending: PendingCommand, rejection: Rejection): Outcome {
  return {
    commandId: pending.commandId,
    status: 'rejected',
    digest: pending.digest,
    response: null,
    eventIds: [],
    rejection: canonicalRejection(rejection),
    identity: pending.identity,
  }
}

/**
 * The check every attempt starts with: the command's outcome row, read strongly consistent. The
 * stored outcome for the same digest, null when there is none, and an
 * {@link IdempotencyKeyReuseError} for another digest.
 */
export async function preCheck(
  repository: OutcomeRepository,
  pending: Pick<PendingCommand, 'commandId' | 'digest'>,
  options: CallOptions = {},
): Promise<Outcome | null> {
  const stored = await repository.outcome(pending.commandId, options)

  return stored === null ? null : sameCommand(stored, pending.digest)
}

/** A command's result read from its stored row. */
export function replayed(outcome: Outcome): CommandOutcome {
  return {outcome, events: [], replayed: true}
}

/**
 * Commits `write`, which carries the command's `outcome`, and settles the result: the outcome as
 * stored with the commit's `events`, or, when the commit failed in a way its row can answer, the
 * stored outcome.
 */
export async function recordCommit(
  repository: OutcomeRepository,
  pending: PendingCommand,
  config: OutcomeConfig,
  outcome: Outcome,
  events: readonly SerializedEvent[],
  write: () => Promise<void>,
): Promise<CommandOutcome> {
  try {
    await write()
  } catch (error) {
    return replayed(await settle(repository, pending, config, error))
  }

  return {
    outcome: storedOutcome(
      outcome,
      events.map((event) => event.id),
    ),
    events,
    replayed: false,
  }
}

/**
 * Records the command as rejected with an outcome-only transaction: the store's checks and the
 * outcome put, with no streams or fences. It settles like any commit that carries an outcome.
 */
export async function persistRejection(
  repository: OutcomeRepository,
  pending: PendingCommand,
  config: OutcomeConfig,
  rejection: Rejection,
  options: CallOptions = {},
): Promise<CommandOutcome> {
  const outcome = rejectedOutcome(pending, rejection)

  return recordCommit(repository, pending, config, outcome, [], () =>
    repository.commitTransaction({streams: [], fences: [], outcome}, options),
  )
}

/**
 * Settles a failed commit that carried the command's outcome, returning the stored outcome that
 * answers it or throwing:
 *
 * - a lost outcome put resolves through the winner's row, or is a retryable `transient` when the
 *   row can't be read within the recovery window;
 * - an indeterminate failure is recovered by polling the row for the recovery window: the same
 *   digest means the commit won, another is an {@link IdempotencyKeyReuseError}, and a row still
 *   absent (or unreadable) is a retryable {@link AmbiguousWriteError}, never "not applied";
 * - a conflict may be the loser of a race for the same command ID, so the row is read once: when
 *   it exists, it answers the command, and when not, the conflict stands;
 * - anything else is thrown as is.
 *
 * The recovery reads ignore the caller's abort signal: a commit that may have applied is resolved
 * before the call returns, within the window.
 */
export async function settle(
  repository: OutcomeRepository,
  pending: Pick<PendingCommand, 'commandId' | 'digest'>,
  config: OutcomeConfig,
  error: unknown,
): Promise<Outcome> {
  const {commandId, digest} = pending

  if (error instanceof OutcomeConflictError) {
    const read = await readBounded(repository, commandId, config.recoveryWaitMs)
    switch (read.kind) {
      case 'found':
        return sameCommand(read.outcome, digest)
      // The row that won the race is gone again, so nothing can be said about the command.
      case 'absent':
        throw error
      case 'failed':
        throw read.error
      // The read stalled past the window. The next attempt's outcome check answers the command.
      case 'timeout':
        throw new TransientError(
          `${error.message}, and its outcome row could not be read in time`,
          {
            cause: error,
          },
        )
    }
  }

  if (error instanceof EvtError && error.code === 'indeterminate') {
    const cause = error.cause ?? error
    const polled = await pollForRow(repository, commandId, config)
    switch (polled.kind) {
      case 'found':
        return sameCommand(polled.outcome, digest)
      case 'corrupt':
        throw polled.error
      case 'absent':
        throw new AmbiguousWriteError(commandId, {cause})
      // The read failure is kept beside the commit's, so an access or schema problem isn't hidden
      // behind "may or may not have been applied".
      case 'unreadable':
        throw new AmbiguousWriteError(commandId, {
          cause:
            polled.error === undefined
              ? cause
              : new AggregateError(
                  [cause, polled.error],
                  `${messageOf(cause)}; reading the outcome row to recover also failed: ` +
                    messageOf(polled.error),
                ),
        })
    }
  }

  if (mayHaveLostTheRace(error)) {
    const read = await readBounded(repository, commandId, config.recoveryWaitMs)
    if (read.kind === 'found') {
      return sameCommand(read.outcome, digest)
    }
    // A malformed row is corruption, not a conflict to retry past.
    if (read.kind === 'failed' && read.error instanceof EvtError) {
      if (read.error.code === 'invalidRecord') {
        throw read.error
      }
    }
  }

  throw error
}

// A conflict whose winner may be another attempt of the same command: its row answers it. A
// fence conflict included, since the winner wrote the stream the loser fenced, and a DynamoDB
// transaction conflict, since the loser was cancelled while the winner held the outcome item.
function mayHaveLostTheRace(error: unknown): boolean {
  return isConflict(error) || (error instanceof TransientError && error.transactionConflict)
}

/** A stored outcome compared with the digest this command computed. */
function sameCommand(stored: Outcome, digest: string): Outcome {
  if (stored.digest !== digest) {
    throw new IdempotencyKeyReuseError(stored.commandId, stored)
  }

  return stored
}

type Read =
  | {kind: 'found'; outcome: Outcome}
  | {kind: 'absent'}
  | {kind: 'failed'; error: unknown}
  | {kind: 'timeout'}

// Reads the outcome row, giving up after the recovery window plus the grace period.
async function readBounded(
  repository: OutcomeRepository,
  commandId: string,
  limitMs: number,
): Promise<Read> {
  const read = async (): Promise<Read> => {
    try {
      const outcome = await repository.outcome(commandId)
      return outcome === null ? {kind: 'absent'} : {kind: 'found', outcome}
    } catch (error) {
      return {kind: 'failed', error}
    }
  }

  return (await within(read(), limitMs + RECOVERY_GRACE_MS)) ?? {kind: 'timeout'}
}

type Polled =
  | {kind: 'found'; outcome: Outcome}
  | {kind: 'absent'}
  | {kind: 'corrupt'; error: unknown}
  // The row couldn't be read: the read failed, or recovery ran out of time (no error).
  | {kind: 'unreadable'; error?: unknown}

/**
 * Polls for the outcome row with exponential backoff until it appears or the recovery window has
 * been slept through. The whole poll, reads included, gets the window plus the grace period, so
 * neither a read that never answers nor a run of slow ones holds recovery past it.
 */
async function pollForRow(
  repository: OutcomeRepository,
  commandId: string,
  config: OutcomeConfig,
): Promise<Polled> {
  const window = config.recoveryWaitMs

  // Set once the deadline answered for the poll, so a read that stalled and then resolves late
  // doesn't go on reading and sleeping after the caller has its answer.
  let stopped = false

  // Each read waits for the backoff after the one before it.
  /* oxlint-disable no-await-in-loop */
  const poll = async (): Promise<Polled> => {
    let delay = RECOVERY_FIRST_DELAY_MS
    let waited = 0
    for (;;) {
      try {
        const outcome = await repository.outcome(commandId)
        if (outcome !== null) {
          return {kind: 'found', outcome}
        }
      } catch (error) {
        if (error instanceof EvtError && error.code === 'invalidRecord') {
          return {kind: 'corrupt', error}
        }
        return {kind: 'unreadable', error}
      }

      if (waited >= window || stopped) {
        return {kind: 'absent'}
      }

      const step = Math.min(delay, window - waited)
      await config.sleep(step)
      waited += step
      delay = Math.min(delay * 2, RECOVERY_MAX_DELAY_MS)
    }
  }
  /* oxlint-enable no-await-in-loop */

  const polled = await within(poll(), window + RECOVERY_GRACE_MS)
  if (polled === undefined) {
    stopped = true
    return {kind: 'unreadable'}
  }

  return polled
}

// Races `work` against a timer of `ms`, resolving undefined when the timer fires first. The timer
// is cleared either way, so it never keeps a runtime alive.
async function within<T>(work: Promise<T>, ms: number): Promise<T | undefined> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const deadline = new Promise<undefined>((resolve) => {
    timer = setTimeout(() => resolve(undefined), ms)
  })
  try {
    return await Promise.race([work, deadline])
  } finally {
    clearTimeout(timer)
  }
}

/**
 * What a failed attempt means for the retry of a command with outcomes:
 *
 * - `never`: throw it.
 * - `transient`: run the command again after a delay, and throw it when the attempts run out: a
 *   throttled backend, a rebuild holding the store, or an ambiguous write says nothing about the
 *   decision going stale.
 * - `conflict`: another writer moved state the attempt read or wrote. Run the command again, and
 *   record a stale-fence rejection when the attempts run out. Stream head drift is never one: it
 *   says the log is damaged, so it's `never`.
 */
export type RetryKind = 'never' | 'transient' | 'conflict'

/** Where in an attempt an error arose, which decides what the retry makes of it. */
export type Phase =
  /** The command's own failure: its handler, an encoding. Never retried. */
  | 'internal'
  /** Reading the outcome row or loading state. */
  | 'deciding'
  /** The commit. */
  | 'committing'

/** How an error from an attempt's `phase` is retried. */
export function retryKind(error: unknown, phase: Phase): RetryKind {
  if (phase === 'internal') {
    return 'never'
  }

  const kind = commitRetryKind(error)

  // A load that raced a compaction finds a sequence gap, which reloading resolves, but it says the
  // read was inconsistent, not that a decision went stale, so it never exhausts into a rejection.
  return phase === 'deciding' && kind === 'conflict' ? 'transient' : kind
}

function commitRetryKind(error: unknown): RetryKind {
  if (error instanceof FenceConflictError) {
    return error.provenance === 'engine' ? 'conflict' : 'never'
  }
  if (isConflict(error)) {
    return 'conflict'
  }
  if (
    error instanceof EvtError &&
    (error.code === 'transient' ||
      error.code === 'ambiguousWrite' ||
      error.code === 'rebuildInProgress' ||
      error.code === 'featureManifestChanged')
  ) {
    return 'transient'
  }

  return 'never'
}

/** The attempts one command has left. */
export class Budget {
  readonly #policy: RetryPolicy
  #attempt = 1

  constructor(policy: RetryPolicy) {
    this.#policy = policy
  }

  /** The attempts made so far, the one that just failed included. */
  get attempts(): number {
    return this.#attempt
  }

  /** Spends one more attempt and returns the delay before it, or null when none are left. */
  next(): number | null {
    if (this.#attempt >= this.#policy.maxAttempts) {
      return null
    }

    const delay = retryDelay(this.#policy, this.#attempt - 1)
    this.#attempt++

    return delay
  }
}
