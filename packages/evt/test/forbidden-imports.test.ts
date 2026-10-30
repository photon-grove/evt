import {describe, expect, it} from 'vitest'

import {forbiddenImports} from '../scripts/forbidden-imports.ts'

describe('forbiddenImports', () => {
  it('finds Node built-ins in every import form', () => {
    const source = [
      "import {readFile} from 'node:fs/promises'",
      'import type {Buffer} from "buffer"',
      "export {join} from 'path'",
      "import 'fs'",
      'const crypto = await import("node:crypto")',
      'const os = await import(`node:os`)',
      "const {inspect} = require('util')",
      "import {\n  createHash,\n} from 'crypto'",
      '/// <reference types="node" />',
    ].join('\n')

    expect(forbiddenImports(source)).toEqual([
      'node:fs/promises',
      'buffer',
      'path',
      'fs',
      'node:crypto',
      'node:os',
      'util',
      'crypto',
      'node',
    ])
  })

  it("refuses other runtimes' modules and packages the core does not depend on", () => {
    const source = [
      "import {Database} from 'bun:sqlite'",
      "import {assert} from 'jsr:@std/assert'",
      "import ulid from 'npm:ulid'",
      "import {DynamoDBClient} from '@aws-sdk/client-dynamodb'",
      "import {z} from 'zod/v4'",
    ].join('\n')

    expect(forbiddenImports(source)).toEqual([
      'bun:sqlite',
      'jsr:@std/assert',
      'npm:ulid',
      '@aws-sdk/client-dynamodb',
      'zod/v4',
    ])
  })

  it('allows relative paths and types references that are not a runtime', () => {
    const source = [
      "import {encode} from './wire/json.js'",
      "export type {Event} from '../types.js'",
      '/// <reference types="vitest/globals" />',
    ].join('\n')

    expect(forbiddenImports(source)).toEqual([])
  })
})
