// Fails when any file under src/ imports a Node built-in, another runtime's module, or a package
// the core may not depend on. The published code runs on Node, Bun, Deno, edge runtimes and
// browsers, so it may use only relative imports and the globals in src/globals.d.ts.
import {readdirSync, readFileSync, statSync} from 'node:fs'
import {join} from 'node:path'
import process from 'node:process'

import {forbiddenImports} from './forbidden-imports.ts'

const src = join(import.meta.dirname, '..', 'src')
const failures: string[] = []

// Every file, not only .ts: tsc also compiles .mts and .cts.
for (const entry of readdirSync(src, {recursive: true, encoding: 'utf8'})) {
  const path = join(src, entry)
  if (!statSync(path).isFile()) {
    continue
  }

  for (const specifier of forbiddenImports(readFileSync(path, 'utf8'))) {
    failures.push(`src/${entry}: ${specifier}`)
  }
}

if (failures.length > 0) {
  console.error('src/ may import only relative paths:\n  ' + failures.join('\n  '))
  process.exit(1)
}
