import {describe, expect, it} from 'vitest'

import {langs} from './content'
import {docBySlug} from './docs'
import {parityHeadlines, parityRows, siteParityRows} from './parity'

describe('parityRows', () => {
  it('finds every headline in the feature matrix, with a status in each language column', () => {
    const rows = siteParityRows()

    expect(rows.map((row) => row.label)).toEqual(parityHeadlines.map((headline) => headline.label))
    for (const row of rows) {
      for (const lang of langs) {
        expect(row.statuses[lang.id], `${row.label} / ${lang.label}`).toBeDefined()
      }
    }
  })

  it('matches each headline to one row only', () => {
    const capabilities = (docBySlug('reference/feature-matrix')?.markdown ?? '')
      .split('\n')
      .filter((line) => line.startsWith('|'))
      .map((line) => line.slice(1).split('|')[0].trim())

    for (const headline of parityHeadlines) {
      const matches = capabilities.filter((capability) => capability.startsWith(headline.row))
      expect(matches, headline.label).toHaveLength(1)
    }
  })

  it('reads the status a cell starts with, by column name', () => {
    const markdown = [
      '| Capability | Tier | Go | Rust | TS |',
      '| --- | --- | --- | --- | --- |',
      '| Widgets (with notes) | core | ✅ | 🚧 (half) | ⏳ (phase 9) |',
    ].join('\n')

    const [row] = parityRows(markdown, [{label: 'Widgets', row: 'Widgets'}])
    expect(row.statuses.go?.kind).toBe('shipped')
    expect(row.statuses.rust?.kind).toBe('progress')
    expect(row.statuses.ts?.kind).toBe('planned')
  })

  it('keeps an escaped pipe inside its cell', () => {
    const markdown = '| Capability | Go | Rust | TS |\n| --- | --- | --- | --- |\n| A \\| B | ✅ | ✅ | ⏳ |'
    const [row] = parityRows(markdown, [{label: 'A or B', row: 'A \\| B'}])
    expect(row.statuses.ts?.kind).toBe('planned')
  })

  it('leaves out a headline whose row is missing', () => {
    const markdown = '| Capability | Go | Rust | TS |\n| --- | --- | --- | --- |\n| Other | ✅ | ✅ | ✅ |'
    expect(parityRows(markdown, [{label: 'Widgets', row: 'Widgets'}])).toEqual([])
  })
})
