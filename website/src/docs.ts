import {humanizeSlug, titleFromMarkdown} from './markdown'

// Every Markdown guide under the repo-root docs/ directory, bundled as a raw string at build time.
// Vite's import.meta.glob keeps these in sync automatically — adding a docs/*.md file surfaces it on
// the site with no code change (its title comes from the first H1, its place from DOC_CONFIG below).
const DOCS_PREFIX = '../../docs/'

const rawDocs = import.meta.glob('../../docs/**/*.md', {
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
const ADRS = 'Architecture decisions'

// Ordering, grouping, and index blurbs for the known docs. Unknown docs still appear (sorted last);
// ADRs are grouped automatically by their adr/ path prefix.
const DOC_CONFIG: Record<string, DocConfig> = {
  'getting-started': {
    group: GUIDES,
    order: 1,
    summary: 'Model an aggregate, commit events, and read it back — in memory first, then DynamoDB.',
  },
  concepts: {
    group: GUIDES,
    order: 2,
    summary: 'Events, entities, snapshots, projections, and the invariants that keep them honest.',
  },
  projections: {
    group: GUIDES,
    order: 3,
    summary:
      'Rebuild views from the log, including incremental delta rebuilds and constant-memory enumeration.',
  },
  streams: {
    group: GUIDES,
    order: 4,
    summary: 'DynamoDB Streams projectors and SNS publishers with idempotency and retry classification.',
  },
  dynamodb: {
    group: GUIDES,
    order: 5,
    summary: 'The single-table event-log layout, key patterns, and conditional-write ordering.',
  },
  postgres: {
    group: GUIDES,
    order: 6,
    summary: 'The relational event-log schema, optimistic concurrency, and snapshot-aware compaction.',
  },
  'integration-cookbook': {
    group: GUIDES,
    order: 7,
    summary: 'Copy-ready patterns for adopters: rebuilds, dedup, schema evolution, and local testing.',
  },
  security: {
    group: GUIDES,
    order: 8,
    summary: 'Keeping a public-safe configuration: neutral examples and local-emulator credentials.',
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
