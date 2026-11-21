// A repository that fails a commit on purpose, so a test can drive ambiguous-write recovery: the
// commit really applies, or is dropped, and either way the caller sees a result it can't trust.

import {EvtError, IndeterminateError} from '../errors.js'
import type {SerializedEvent, SerializedSnapshot} from '../event.js'
import type {Outcome} from '../outcome.js'
import {
  type CallOptions,
  type Commit,
  type CommitReceipt,
  type OutcomeRepository,
  type Repository,
  type TransactionCommit,
  type TransactionalRepository,
  commitTransactionWithReceipt,
  commitWithReceipt,
} from '../repository.js'

/**
 * Wraps an {@link OutcomeRepository} and lets a test intercept its next commit (`commit` or
 * `commitTransaction`, or either with a receipt): {@link applyThenFail} applies it and then throws
 * an {@link IndeterminateError}, and {@link failWithoutApplying} drops it and throws one. Each arms
 * one commit and disarms itself. Everything else, the outcome read included, is the wrapped
 * repository's.
 */
export class FaultRepository implements OutcomeRepository {
  readonly inner: OutcomeRepository
  /** The wrapped repository's declaration, present only when it declares. */
  declare readonly declareHistoryDependent?: (
    types: readonly string[],
    options?: CallOptions,
  ) => Promise<void>
  #fault: {apply: boolean; cause: unknown} | null = null

  constructor(inner: OutcomeRepository) {
    if (typeof inner.outcome !== 'function' || typeof inner.commitTransaction !== 'function') {
      throw new EvtError('invalidArgument', 'a FaultRepository wraps an OutcomeRepository')
    }

    this.inner = inner
    // A capability the store tests for, so it's here only when the wrapped repository has it.
    const declare = inner.declareHistoryDependent
    if (declare !== undefined) {
      this.declareHistoryDependent = (types, options) => declare.call(inner, types, options)
    }
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

  /**
   * Forwards to the wrapped repository's own receipt method. When it has none, the commit goes
   * through {@link commit}, so a subclass that overrides only `commit` still sees the store's
   * commits. Over a repository with receipt methods, such as a journal's, the store commits through
   * them alone: a subclass that intercepts commits must override them too.
   */
  commitWithReceipt(commit: Commit, options?: CallOptions): Promise<CommitReceipt> {
    if (typeof this.inner.commitWithReceipt !== 'function') {
      return commitWithReceipt({commit: (c, o) => this.commit(c, o)} as Repository, commit, options)
    }

    return this.#intercept(() => commitWithReceipt(this.inner, commit, options))
  }

  commitTransaction(transaction: TransactionCommit, options?: CallOptions): Promise<void> {
    return this.#intercept(() => this.inner.commitTransaction(transaction, options))
  }

  /** Forwards like {@link commitWithReceipt}, through {@link commitTransaction} as its fallback. */
  commitTransactionWithReceipt(
    transaction: TransactionCommit,
    options?: CallOptions,
  ): Promise<CommitReceipt> {
    if (typeof this.inner.commitTransactionWithReceipt !== 'function') {
      const through = {
        commitTransaction: (t: TransactionCommit, o?: CallOptions) => this.commitTransaction(t, o),
      }
      return commitTransactionWithReceipt(through as TransactionalRepository, transaction, options)
    }

    return this.#intercept(() => commitTransactionWithReceipt(this.inner, transaction, options))
  }

  writesViews(): boolean {
    return this.inner.writesViews?.() === true
  }

  appliesExpectedPayload(): boolean {
    return this.inner.appliesExpectedPayload?.() === true
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

  async #intercept<T>(write: () => Promise<T>): Promise<T> {
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
