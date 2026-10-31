// A framework-free check runner. Bun, Deno and every supported Node run it unchanged, which a
// test framework would not allow (ADR 0009, section 3).
import {readFile} from 'node:fs/promises'
import process from 'node:process'

import {JsonNumber, type JsonValue, parseJson, stringifyJson} from '@photon-grove/evt'

// The repo's compat/fixtures directory, from packages/evt-compat/dist/.
const FIXTURES = new URL('../../../compat/fixtures/', import.meta.url)

// Fixtures are read with evt's own lossless reader: JSON.parse would round 2^53 + 1.
export async function loadFixture(name: string): Promise<JsonValue> {
  return parseJson(await readFile(new URL(name, FIXTURES), 'utf8'))
}

/** A field of a fixture object. */
export function field(value: JsonValue | undefined, name: string): JsonValue {
  if (!(value instanceof Map)) {
    throw new Error(`fixture has no object for ${name}`)
  }

  const found = value.get(name)
  if (found === undefined) {
    throw new Error(`fixture has no field ${name}`)
  }

  return found
}

export function str(value: JsonValue): string {
  if (typeof value !== 'string') {
    throw new Error(`fixture value is not a string: ${stringifyJson(value)}`)
  }

  return value
}

export function int(value: JsonValue): bigint {
  if (!(value instanceof JsonNumber) || !value.isInteger) {
    throw new Error(`fixture value is not an integer: ${stringifyJson(value)}`)
  }

  return BigInt(value.text)
}

export function num(value: JsonValue): number {
  return Number(int(value))
}

export function list(value: JsonValue): JsonValue[] {
  if (!Array.isArray(value)) {
    throw new Error(`fixture value is not an array: ${stringifyJson(value)}`)
  }

  return value
}

/** A fixture object as a plain object, recursively, such as a DynamoDB item. */
export function plain(value: JsonValue): unknown {
  if (value instanceof Map) {
    return Object.fromEntries(Array.from(value, ([key, entry]) => [key, plain(entry)]))
  }
  if (Array.isArray(value)) {
    return value.map(plain)
  }

  return value
}

/** Canonical JSON for comparing documents whose key order doesn't matter, such as items. */
export function canonical(value: unknown): string {
  return stringifyJson(sorted(value))
}

function sorted(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sorted)
  }
  if (value instanceof Map) {
    return new Map(Array.from(value, ([key, entry]: [unknown, unknown]) => [key, sorted(entry)]))
  }
  const isStruct = typeof value === 'object' && value !== null
  if (isStruct && Object.getPrototypeOf(value) === Object.prototype) {
    return new Map(Object.entries(value).map(([key, entry]) => [key, sorted(entry)]))
  }

  return value
}

export class Checks {
  private passed = 0
  private readonly failures: string[] = []

  equal(name: string, got: unknown, want: unknown): void {
    if (got === want) {
      this.passed++
      return
    }

    this.failures.push(`${name}\n    got  ${String(got)}\n    want ${String(want)}`)
  }

  // run records a check that throws as a failure, so one bad vector doesn't hide the rest.
  run(name: string, check: () => void): void {
    try {
      check()
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      this.failures.push(`${name}\n    threw ${message}`)
    }
  }

  // report prints the result and sets a failing exit code when any check failed.
  report(): void {
    for (const failure of this.failures) {
      console.error(`FAIL ${failure}`)
    }

    console.log(`${runtime()}: ${this.passed} passed, ${this.failures.length} failed`)
    if (this.failures.length > 0) {
      process.exitCode = 1
    }
  }
}

function runtime(): string {
  const versions = process.versions as Record<string, string | undefined>
  if (versions['bun'] !== undefined) {
    return `bun ${versions['bun']}`
  }
  if (versions['deno'] !== undefined) {
    return `deno ${versions['deno']}`
  }

  return `node ${process.versions.node}`
}
