// Errors.

/** Why a record that is meant for this consumer cannot be read. */
export type DecodeErrorKind = 'missingNewImage' | 'invalidRecord'

/**
 * A record that is meant for this consumer and cannot be read.
 *
 * A decode error is never skipped: the batch helpers report the record as failed, so the event
 * source mapping retries it and, once its retries run out, sends it to the on-failure destination.
 *
 * - `missingNewImage`: an event item was inserted, but the record has no new image. The table's
 *   stream view type is `KEYS_ONLY` or `OLD_IMAGE`, and event decode needs `NEW_IMAGE` or
 *   `NEW_AND_OLD_IMAGES`. `viewType` is the view type the record reports, or `unset`.
 * - `invalidRecord`: the event item does not decode. `cause` says why.
 */
export class DecodeError extends Error {
  override readonly name = 'DecodeError'
  readonly kind: DecodeErrorKind
  /** The sequence number of the record that failed. */
  readonly sequenceNumber: string
  /** For `missingNewImage`, the view type the record reports, or `unset`. */
  readonly viewType: string | undefined

  private constructor(
    kind: DecodeErrorKind,
    sequenceNumber: string,
    message: string,
    options: {viewType?: string; cause?: unknown},
  ) {
    super(message, options.cause === undefined ? undefined : {cause: options.cause})
    this.kind = kind
    this.sequenceNumber = sequenceNumber
    this.viewType = options.viewType
  }

  /** An event insert without a new image. */
  static missingNewImage(sequenceNumber: string, viewType: string): DecodeError {
    return new DecodeError(
      'missingNewImage',
      sequenceNumber,
      `stream record ${sequenceNumber} inserts an event but has no NewImage; the stream view type ` +
        `must be NEW_IMAGE or NEW_AND_OLD_IMAGES (it is ${viewType})`,
      {viewType},
    )
  }

  /** An event item that does not decode. */
  static invalidRecord(sequenceNumber: string, cause: unknown): DecodeError {
    const reason = cause instanceof Error ? cause.message : String(cause)

    return new DecodeError(
      'invalidRecord',
      sequenceNumber,
      `stream record ${sequenceNumber}: ${reason}`,
      {
        cause,
      },
    )
  }
}

/** Why a filter cannot be turned into an event source mapping filter. */
export type FilterErrorKind = 'empty' | 'tooLong'

/**
 * A filter that cannot be turned into an event source mapping filter.
 *
 * - `empty`: an empty value or prefix, which would match nothing or everything. `what` names it.
 * - `tooLong`: the pattern is longer than Lambda accepts. `length` is its length in UTF-8 bytes,
 *   and `max` is {@link MAX_PATTERN_BYTES}.
 */
export class FilterError extends Error {
  override readonly name = 'FilterError'
  readonly kind: FilterErrorKind
  /** For `empty`, what was empty, such as `event type`. */
  readonly what: string | undefined
  /** For `tooLong`, the pattern's length in UTF-8 bytes. */
  readonly length: number | undefined
  /** For `tooLong`, the most bytes Lambda accepts. */
  readonly max: number | undefined

  private constructor(
    kind: FilterErrorKind,
    message: string,
    fields: {what?: string; length?: number; max?: number},
  ) {
    super(message)
    this.kind = kind
    this.what = fields.what
    this.length = fields.length
    this.max = fields.max
  }

  /** An empty value or prefix. */
  static empty(what: string): FilterError {
    return new FilterError('empty', `a filter ${what} cannot be empty`, {what})
  }

  /** A pattern longer than Lambda accepts. */
  static tooLong(length: number, max: number): FilterError {
    return new FilterError(
      'tooLong',
      `the filter pattern is ${length} bytes; Lambda accepts at most ${max}`,
      {
        length,
        max,
      },
    )
  }
}
