import {humanizeSlug, titleFromMarkdown} from './markdown'

// Every Markdown guide under the repo-root docs/ directory (except docs/internal/), bundled as a raw string at build time.
// Vite's import.meta.glob keeps these in sync automatically — adding a docs/*.md file surfaces it on
// the site with no code change (its title comes from the first H1, its place from DOC_CONFIG below).
const DOCS_PREFIX = '../../docs/'

const rawDocs = import.meta.glob(['../../docs/**/*.md', '!../../docs/internal/**'], {
  query: '?raw',
  import: 'default',
  eager: true,
}) as Record<string, string>

export interface DocMeta {
  slug: string
  title: string
  summary: string
  group: string
  order: number
  markdown: string
}

export interface DocGroup {
  name: string
  docs: DocMeta[]
}

interface DocConfig {
  group: string
  order: number
  summary: string
}

const GUIDES = 'Guides'
const REFERENCE = 'Reference'
const ADRS = 'Architecture decisions'

// Ordering, grouping, and index blurbs for the known docs. Unknown docs still appear (sorted last);
// ADRs are grouped automatically by their adr/ path prefix.
const DOC_CONFIG: Record<string, DocConfig> = {
  'guide/getting-started': {
    group: GUIDES,
    order: 1,
    summary: 'Model an aggregate, commit events and read it back, in memory first and then on DynamoDB.',
  },
  'guide/concepts': {
    group: GUIDES,
    order: 2,
    summary: 'Aggregates, commands, events, snapshots, projections and the rules that tie them together.',
  },
  'guide/examples': {
    group: GUIDES,
    order: 2.5,
    summary: 'Every runnable example in both languages, what it shows, and the command that runs it.',
  },
  'guide/projections': {
    group: GUIDES,
    order: 3,
    summary: 'Rebuild views from the log, fence rebuilds against live writers, and write cross-entity views in the commit\'s own transaction.',
  },
  'guide/streams': {
    group: GUIDES,
    order: 4,
    summary: 'Read the DynamoDB Stream with idempotent projectors, skip head items, and publish onward from your own handler.',
  },
  'guide/dynamodb': {
    group: GUIDES,
    order: 5,
    summary: 'The single-table event log, what a commit sends, and the snapshot, compaction and retention options.',
  },
  'guide/postgres': {
    group: GUIDES,
    order: 6,
    summary: 'The relational schema, the engine tables EnsureSchema creates, and view writers that enforce row owners.',
  },
  'guide/commands': {
    group: GUIDES,
    order: 9,
    summary: 'Durable command outcomes, rejections that carry events, and fence-aware retry.',
  },
  'guide/upgrading-to-v0.9': {
    group: GUIDES,
    order: 10,
    summary: 'What v0.9 removed, what breaks at compile time, and what a custom repository must honor.',
  },
  'guide/integration': {
    group: GUIDES,
    order: 7,
    summary: 'Integration rules and the reasons behind them: rebuilds, retry keys, schema evolution and local testing.',
  },
  'guide/migrating-to-stream-heads': {
    group: GUIDES,
    order: 8,
    summary: 'The runbook for moving a v0.8 store to per-stream head items: stop writers, back up, backfill, verify, declare.',
  },
  'reference/storage-format': {
    group: REFERENCE,
    order: 20,
    summary: 'The normative byte-level format that Go and Rust both read and write.',
  },
  'reference/feature-matrix': {
    group: REFERENCE,
    order: 21,
    summary: 'Which features each language implements today.',
  },
}

function metaFor(path: string, markdown: string): DocMeta {
  const slug = slugFromPath(path)
  const config = DOC_CONFIG[slug]
  const isAdr = slug.startsWith('adr/')

  const title = titleFromMarkdown(markdown) ?? humanizeSlug(slug)
  const summary = config?.summary ?? (isAdr ? 'Architecture decision record.' : '')
  const group = config?.group ?? (isAdr ? ADRS : GUIDES)
  const order = config?.order ?? 99

  return {slug, title, summary, group, order, markdown}
}

// slugFromPath turns a glob key ("../../docs/adr/0001-x.md") into a route slug ("adr/0001-x").
export function slugFromPath(path: string): string {
  const trimmed = path.startsWith(DOCS_PREFIX) ? path.slice(DOCS_PREFIX.length) : path

  return trimmed.replace(/\.md$/, '')
}

export const docs: DocMeta[] = Object.entries(rawDocs)
  .map(([path, markdown]) => metaFor(path, markdown))
  .sort((a, b) => a.order - b.order || a.title.localeCompare(b.title))

export function docBySlug(slug: string): DocMeta | undefined {
  return docs.find((doc) => doc.slug === slug)
}

// docGroups returns docs bucketed by group, each bucket preserving the global order, with the
// Guides bucket first.
export function docGroups(): DocGroup[] {
  const groups: DocGroup[] = []

  for (const doc of docs) {
    const existing = groups.find((group) => group.name === doc.group)
    if (existing) {
      existing.docs.push(doc)
    } else {
      groups.push({name: doc.group, docs: [doc]})
    }
  }

  return groups.sort((a, b) => (a.name === GUIDES ? -1 : b.name === GUIDES ? 1 : 0))
}
