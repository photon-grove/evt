// A framework-free check runner. Bun, Deno and every supported Node run it unchanged, which a
// test framework would not allow (ADR 0009, section 3).
import {readFile} from 'node:fs/promises'
import process from 'node:process'

// The repo's compat/fixtures directory, from packages/evt-compat/dist/.
const FIXTURES = new URL('../../../compat/fixtures/', import.meta.url)

export async function loadFixture(name: string): Promise<unknown> {
  return JSON.parse(await readFile(new URL(name, FIXTURES), 'utf8'))
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
