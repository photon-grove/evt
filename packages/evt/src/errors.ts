// Every failure evt throws is an EvtError with a code, as the Rust crate returns its Error enum.
// Domain outcomes (a rejected command, a duplicate) are values, never errors.

/**
 * What went wrong:
 *
 * - `invalidJson`: text that isn't JSON.
 * - `invalidRecord`: a stored record (an event, a snapshot, an item) with the wrong shape.
 * - `invalidArgument`: a value evt can't write, such as a float that isn't finite or an object
 *   whose key order the engine has already changed.
 */
export type EvtErrorCode = 'invalidJson' | 'invalidRecord' | 'invalidArgument'

export class EvtError extends Error {
  override readonly name = 'EvtError'
  readonly code: EvtErrorCode

  constructor(code: EvtErrorCode, message: string, options?: {cause?: unknown}) {
    super(message, options)
    this.code = code
  }
}
