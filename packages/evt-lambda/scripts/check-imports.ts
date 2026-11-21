// Fails when any file under src/ imports a Node built-in, another runtime's module, or a package
// other than @photon-grove/evt. The published code runs wherever @photon-grove/evt does, so it may
// use only relative imports, @photon-grove/evt's entry points, and the globals in src/globals.d.ts.
// The specifier rules are packages/evt's (scripts/forbidden-imports.ts), with one allowed package.
import {readdirSync, readFileSync, statSync} from 'node:fs'
import {isBuiltin} from 'node:module'
import {join} from 'node:path'
import process from 'node:process'

const SPECIFIER = /(?:\bfrom\s*|\bimport\s*\(?\s*|\brequire\s*\(\s*)(['"`])([^'"`\n]+)\1/g

const TYPES_REFERENCE = /\/\/\/\s*<reference\s+types\s*=\s*(['"])([^'"\n]+)\1/g

const ALLOWED_PACKAGES: ReadonlySet<string> = new Set(['@photon-grove/evt'])

const src = join(import.meta.dirname, '..', 'src')
const failures: string[] = []

for (const entry of readdirSync(src, {recursive: true, encoding: 'utf8'})) {
  const path = join(src, entry)
  if (!statSync(path).isFile()) {
    continue
  }

  const source = readFileSync(path, 'utf8')
  for (const match of source.matchAll(SPECIFIER)) {
    const specifier = match[2]!
    if (!isRelative(specifier) && !ALLOWED_PACKAGES.has(packageName(specifier))) {
      failures.push(`src/${entry}: ${specifier}`)
    }
  }
  for (const match of source.matchAll(TYPES_REFERENCE)) {
    const types = match[2]!
    if (types === 'node' || types.startsWith('node/') || isBuiltin(types)) {
      failures.push(`src/${entry}: ${types}`)
    }
  }
}

if (failures.length > 0) {
  console.error(
    'src/ may import only relative paths and @photon-grove/evt:\n  ' + failures.join('\n  '),
  )
  process.exit(1)
}

function isRelative(specifier: string): boolean {
  return specifier.startsWith('./') || specifier.startsWith('../')
}

function packageName(specifier: string): string {
  const parts = specifier.split('/')

  return specifier.startsWith('@') ? parts.slice(0, 2).join('/') : parts[0]!
}
