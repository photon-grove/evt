import {isBuiltin} from 'node:module'

// Matches the specifier of a static import or re-export, a side-effect import, a dynamic import()
// (with a plain or template string), and a require() call. It reads comments too, which only errs
// toward refusing.
const SPECIFIER = /(?:\bfrom\s*|\bimport\s*\(?\s*|\brequire\s*\(\s*)(['"`])([^'"`\n]+)\1/g

// A triple-slash types reference pulls a runtime's ambient types into the program.
const TYPES_REFERENCE = /\/\/\/\s*<reference\s+types\s*=\s*(['"])([^'"\n]+)\1/g

// The packages src/ may import. The core has no runtime dependencies, so it's empty. A subpath
// built on an optional peer dependency, such as /dynamodb, adds that package here.
const ALLOWED_PACKAGES: ReadonlySet<string> = new Set()

// forbiddenImports returns each specifier in source that the published code must not import, in
// order: Node built-ins (`node:fs`, `fs`, `fs/promises`), other runtimes' schemes (`bun:sqlite`,
// `npm:x`, `jsr:@std/x`), any package outside ALLOWED_PACKAGES, and runtime types references such
// as `node`. Relative paths are always allowed.
export function forbiddenImports(source: string): string[] {
  const found: string[] = []

  for (const match of source.matchAll(SPECIFIER)) {
    const specifier = match[2]!
    if (!isRelative(specifier) && !ALLOWED_PACKAGES.has(packageName(specifier))) {
      found.push(specifier)
    }
  }

  for (const match of source.matchAll(TYPES_REFERENCE)) {
    const types = match[2]!
    if (types === 'node' || types.startsWith('node/') || isBuiltin(types)) {
      found.push(types)
    }
  }

  return found
}

function isRelative(specifier: string): boolean {
  return specifier.startsWith('./') || specifier.startsWith('../')
}

// packageName is the package a bare specifier names: `@scope/name` or `name`, without a subpath.
function packageName(specifier: string): string {
  const parts = specifier.split('/')

  return specifier.startsWith('@') ? parts.slice(0, 2).join('/') : parts[0]!
}
