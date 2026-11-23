// The runtime globals the published code may see: web-standard APIs that every runtime
// @photon-grove/evt supports provides (ADR 0009, section 3). tsconfig.json loads no DOM or Node
// types, so anything else, such as Buffer or process, is a type error. Declare a global here only
// when the code needs it and every target runtime has it, typed no looser than the runtimes' own
// declarations, as packages/evt/src/globals.d.ts does: tsconfig.test.json checks src/ against
// Node's types instead.

// The package itself uses TextEncoder (a counter's payload as the journal's decoder reads it), and
// @photon-grove/evt's declarations name AbortSignal (its call options take one), so the compiler
// needs the type to read them.
declare class TextEncoder {
  readonly encoding: string
  encode(input?: string): Uint8Array<ArrayBuffer>
}

interface AbortSignal {
  readonly aborted: boolean
  readonly reason: unknown
  throwIfAborted(): void
  addEventListener(type: 'abort', listener: () => void, options?: {once?: boolean}): void
  removeEventListener(type: 'abort', listener: () => void): void
}
