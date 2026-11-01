// The only runtime globals the published code may use: web-standard APIs that Node, Bun, Deno,
// edge runtimes and browsers all provide (ADR 0009, section 3). tsconfig.json loads no DOM or Node
// types, so anything else, such as Buffer, process or document, is a type error. Declare a global
// here only when the code needs it and every target runtime has it, typed no looser than the
// runtimes' own declarations: tsconfig.test.json checks src/ against Node's types instead.

declare class TextEncoder {
  readonly encoding: string
  encode(input?: string): Uint8Array<ArrayBuffer>
}

declare class TextDecoder {
  constructor(label?: string, options?: {fatal?: boolean; ignoreBOM?: boolean})
  readonly encoding: string
  readonly fatal: boolean
  decode(input?: ArrayBuffer | ArrayBufferView, options?: {stream?: boolean}): string
}

interface AbortSignal {
  readonly aborted: boolean
  readonly reason: unknown
  throwIfAborted(): void
  addEventListener(type: 'abort', listener: () => void, options?: {once?: boolean}): void
  removeEventListener(type: 'abort', listener: () => void): void
}

// Browsers define crypto.subtle only in secure contexts (HTTPS or localhost).
declare var crypto: {
  getRandomValues<T extends Uint8Array>(array: T): T
  readonly subtle: {
    digest(algorithm: 'SHA-256', data: Uint8Array<ArrayBuffer>): Promise<ArrayBuffer>
  }
}

declare function setTimeout(callback: () => void, ms?: number): unknown

declare function clearTimeout(id: unknown): void

declare function queueMicrotask(callback: () => void): void
