// Keeps the TypeScript walkthrough in docs/guide/getting-started.md honest. Every `ts` code block
// in the guide must appear verbatim in examples/getting-started.ts or its test file, which CI
// type-checks and runs, so a snippet can't drift from code that works.
import {readFileSync} from 'node:fs'
import {join} from 'node:path'

import {expect, test} from 'vitest'

import {openAndDeposit} from '../examples/getting-started.ts'

const root = join(import.meta.dirname, '..')

test('the guide takes its TypeScript blocks from the example', () => {
  const guide = readFileSync(join(root, '../../docs/guide/getting-started.md'), 'utf8')
  const example = ['getting-started.ts', 'getting-started.test.ts']
    .map((file) => readFileSync(join(root, 'examples', file), 'utf8'))
    .join('\n')

  // A TypeScript or JavaScript block fenced any other way (indented, `~~~`, `typescript`, `js`)
  // would skip the check below.
  const fence = /^\s*(```|~~~)\s*(ts|typescript|tsx|mts|cts|js|javascript|jsx|mjs|cjs)\b/i
  const fences = guide.split('\n').filter((line) => fence.test(line))
  expect(fences.filter((line) => line !== '```ts')).toEqual([])

  const blocks = tsBlocks(guide)
  // Update this count after checking the new blocks.
  expect(blocks).toHaveLength(7)

  for (const block of blocks) {
    expect(example.includes(block), `not in the example verbatim:\n${block}`).toBe(true)
  }
})

test('the example program opens and deposits', async () => {
  const loaded = await openAndDeposit()

  // The guide says so of this load.
  expect(loaded.sequence).toBe(2n)
  expect(loaded.state).toEqual({id: 'acct-1', balance: 125, opened: true})
})

// The body of every fenced block opened with exactly ```ts.
function tsBlocks(markdown: string): string[] {
  const blocks: string[] = []
  let current: string[] | null = null

  for (const line of markdown.split('\n')) {
    if (current === null) {
      if (line === '```ts') {
        current = []
      }
    } else if (line === '```') {
      blocks.push(current.join('\n'))
      current = null
    } else {
      current.push(line)
    }
  }

  return blocks
}
