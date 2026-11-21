import type {Lang} from './content'
import {docBySlug} from './docs'
import {type Status, statusOf} from './markdown'

// The landing page's parity grid, read from docs/reference/feature-matrix.md at build time so it
// can't disagree with the matrix. Each headline names the matrix row it summarizes by the start of
// that row's Capability cell.
export interface ParityHeadline {
  label: string
  row: string
}

export const parityHeadlines: ParityHeadline[] = [
  {label: 'Events, codecs and metadata', row: 'Core types and event model'},
  {label: 'Upcasting on read', row: 'Upcasting on read'},
  {label: 'Inline snapshots', row: 'Inline snapshots'},
  {label: 'In-memory backend', row: 'In-memory backend'},
  {label: 'DynamoDB backend', row: 'DynamoDB backend'},
  {label: 'PostgreSQL backend', row: 'PostgreSQL backend'},
  {label: 'Multi-stream transactions', row: 'Multi-stream transactions'},
  {label: 'Read fences', row: 'Read fences'},
  {label: 'Durable command outcomes', row: 'Command outcomes ('},
  {label: 'Fence-aware retry', row: 'Fence-aware retry'},
  {label: 'Commit-wide projectors', row: 'Commit-wide projectors'},
  {label: 'Fenced projection rebuilds', row: 'Full rebuild ('},
  {label: 'View repository', row: 'View repository'},
  {label: 'Sync journal', row: 'Journal (scope'},
  {label: 'Lambda stream helpers', row: '`photon-evt-lambda`'},
]

export interface ParityRow {
  label: string
  statuses: Record<Lang, Status | undefined>
}

const COLUMNS: Record<Lang, string> = {go: 'Go', rust: 'Rust', ts: 'TS'}

function cellsOf(line: string): string[] {
  return line
    .trim()
    .replace(/^\|/, '')
    .replace(/\|$/, '')
    .split(/(?<!\\)\|/)
    .map((cell) => cell.trim())
}

// parityRows finds each headline's row in the matrix's Markdown and reads the status each language
// column starts with. A headline whose row is missing is left out, and the test catches it.
export function parityRows(markdown: string, headlines: ParityHeadline[] = parityHeadlines): ParityRow[] {
  const lines = markdown.split('\n').filter((line) => line.trimStart().startsWith('|'))
  const header = lines.map(cellsOf).find((cells) => cells[0] === 'Capability')
  if (!header) {
    return []
  }

  const index = (column: string) => header.indexOf(column)
  const rows = lines.map(cellsOf)

  return headlines.flatMap((headline) => {
    const cells = rows.find((row) => row[0]?.startsWith(headline.row))
    if (!cells) {
      return []
    }

    const statuses = Object.fromEntries(
      (Object.keys(COLUMNS) as Lang[]).map((lang) => [lang, statusOf(cells[index(COLUMNS[lang])] ?? '')]),
    ) as Record<Lang, Status | undefined>

    return [{label: headline.label, statuses}]
  })
}

export function siteParityRows(): ParityRow[] {
  const matrix = docBySlug('reference/feature-matrix')

  return matrix ? parityRows(matrix.markdown) : []
}
