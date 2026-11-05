// A repository that fails a commit on purpose, so a test can drive ambiguous-write recovery: the
// commit really applies, or is dropped, and either way the caller sees a result it can't trust.

import {EvtError, IndeterminateError} from '../errors.js'
import type {SerializedEvent, SerializedSnapshot} from '../event.js'
import type {Outcome} from '../outcome.js'
import type {CallOptions, Commit, OutcomeRepository, TransactionCommit} from '../repository.js'

/**
 * Wraps an {@link OutcomeRepository} and lets a test intercept its next commit (`commit` or
 * `commitTransaction`): {@link applyThenFail} applies it and then throws an
 * {@link IndeterminateError}, and {@link failWithoutApplying} drops it and throws one. Each arms
 * one commit and disarms itself. Everything else, the outcome read included, is the wrapped
 * repository's.
 */
export class FaultRepository implements OutcomeRepository {
  readonly inner: OutcomeRepository
  #fault: {apply: boolean; cause: unknown} | null = null

  constructor(inner: OutcomeRepository) {
    if (typeof inner.outcome !== 'function' || typeof inner.commitTransaction !== 'function') {
      throw new EvtError('invalidArgument', 'a FaultRepository wraps an OutcomeRepository')
    }

    this.inner = inner
  }

  /** The next commit applies, and then throws an `IndeterminateError` with `cause`. */
  applyThenFail(cause: unknown = new Error('injected commit fault')): this {
    this.#fault = {apply: true, cause}

    return this
  }

  /** The next commit is dropped, and throws an `IndeterminateError` with `cause`. */
  failWithoutApplying(cause: unknown = new Error('injected commit fault')): this {
    this.#fault = {apply: false, cause}

    return this
  }

  commit(commit: Commit, options?: CallOptions): Promise<void> {
    return this.#intercept(() => this.inner.commit(commit, options))
  }

  commitTransaction(transaction: TransactionCommit, options?: CallOptions): Promise<void> {
    return this.#intercept(() => this.inner.commitTransaction(transaction, options))
  }

  outcome(commandId: string, options?: CallOptions): Promise<Outcome | null> {
    return this.inner.outcome(commandId, options)
  }

  events(entityId: string, options?: CallOptions): Promise<SerializedEvent[]> {
    return this.inner.events(entityId, options)
  }

  eventsAfter(entityId: string, after: bigint, options?: CallOptions): Promise<SerializedEvent[]> {
    return this.inner.eventsAfter(entityId, after, options)
  }

  latest(entityId: string, n: number, options?: CallOptions): Promise<SerializedEvent[]> {
    return this.inner.latest(entityId, n, options)
  }

  snapshot(entityId: string, options?: CallOptions): Promise<SerializedSnapshot | null> {
    return this.inner.snapshot(entityId, options)
  }

  putSnapshot(snapshot: SerializedSnapshot, options?: CallOptions): Promise<boolean> {
    return this.inner.putSnapshot(snapshot, options)
  }

  compactBelow(entityId: string, through: bigint, options?: CallOptions): Promise<number> {
    return this.inner.compactBelow(entityId, through, options)
  }

  async #intercept(write: () => Promise<void>): Promise<void> {
    const fault = this.#fault
    if (fault === null) {
      return write()
    }

    this.#fault = null
    if (fault.apply) {
      await write()
    }

    throw new IndeterminateError({cause: fault.cause})
  }
}
