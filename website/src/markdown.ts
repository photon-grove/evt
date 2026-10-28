// Pure helpers for turning the repo's Markdown docs into in-site content. Kept free of DOM and
// Vite globals so they can be unit-tested directly.

// slugifyHeading mirrors GitHub's heading-anchor scheme closely enough for our docs: lowercase,
// drop punctuation other than spaces and hyphens, then collapse whitespace to single hyphens. It
// must agree with the in-page anchor links the docs already use (e.g. #constant-memory-enumeration).
export function slugifyHeading(text: string): string {
  return text
    .toLowerCase()
    .trim()
    .replace(/[^\w\s-]/g, '')
    .replace(/\s+/g, '-')
}

// titleFromMarkdown returns the text of the first level-1 heading, the conventional document title.
export function titleFromMarkdown(markdown: string): string | undefined {
  const match = markdown.match(/^#\s+(.+?)\s*$/m)

  return match ? match[1].trim() : undefined
}

// humanizeSlug is the last-resort title for a doc with no H1: turn the final path segment into
// Title Case (e.g. "getting-started" -> "Getting Started").
export function humanizeSlug(slug: string): string {
  const last = slug.split('/').pop() ?? slug

  return last.replace(/[-_]/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())
}

// DocLink classifies an anchor href found inside a rendered doc so the viewer can route it.
export type DocLink =
  | {kind: 'external'; href: string}
  | {kind: 'route'; slug: string}
  | {kind: 'anchor'; id: string}
  | {kind: 'repo'; path: string}
  | {kind: 'asis'; href: string}

// resolveDocLink decides how an href inside the doc identified by fromSlug should behave:
//   - absolute/protocol URLs open externally,
//   - pure "#fragment" links scroll within the current doc,
//   - relative links to another ".md" under docs/ become an in-site doc route,
//   - other relative links (source files, directories, Markdown outside docs/) open on GitHub,
//   - anything that climbs above the repo root is left untouched.
export function resolveDocLink(href: string, fromSlug: string): DocLink {
  if (/^[a-z][a-z0-9+.-]*:/i.test(href) || href.startsWith('//')) {
    return {kind: 'external', href}
  }

  if (href.startsWith('#')) {
    return {kind: 'anchor', id: href.slice(1)}
  }

  const [pathPart, fragment] = href.split('#')
  const repoPath = resolveRepoPath(fromSlug, pathPart)
  if (repoPath === undefined) {
    return {kind: 'asis', href}
  }

  if (repoPath.startsWith('docs/') && repoPath.endsWith('.md')) {
    return {kind: 'route', slug: repoPath.slice('docs/'.length).replace(/\.md$/, '')}
  }

  return {kind: 'repo', path: fragment ? `${repoPath}#${fragment}` : repoPath}
}

// resolveRepoPath resolves target (a relative path like "adr/x.md" or "../../go/errors.go")
// against the docs/ directory of fromSlug, returning a repo-root-relative path, or undefined when
// the target climbs above the repo root.
function resolveRepoPath(fromSlug: string, target: string): string | undefined {
  const baseDir = fromSlug.includes('/') ? fromSlug.slice(0, fromSlug.lastIndexOf('/')) : ''
  const parts = ['docs', ...(baseDir ? baseDir.split('/') : [])]

  for (const segment of target.split('/')) {
    if (segment === '' || segment === '.') {
      continue
    }

    if (segment === '..') {
      if (parts.length === 0) {
        return undefined
      }
      parts.pop()
    } else {
      parts.push(segment)
    }
  }

  return parts.join('/')
}

// Status is one feature-matrix cell marker, as the matrix legend defines them.
export interface Status {
  symbol: string
  kind: 'shipped' | 'progress' | 'planned' | 'deferred' | 'cut' | 'na'
  label: string
}

const STATUSES: Status[] = [
  {symbol: '✅', kind: 'shipped', label: 'Shipped'},
  {symbol: '🚧', kind: 'progress', label: 'In progress'},
  {symbol: '⏳', kind: 'planned', label: 'Planned'},
  {symbol: '⏸', kind: 'deferred', label: 'Deferred'},
  {symbol: '🗑️', kind: 'cut', label: 'Cut'},
  {symbol: '🗑', kind: 'cut', label: 'Cut'},
  {symbol: '—', kind: 'na', label: 'Not applicable'},
]

// statusOf returns the status a matrix cell starts with, if any.
export function statusOf(cell: string): Status | undefined {
  const text = cell.trimStart()

  return STATUSES.find((status) => text.startsWith(status.symbol))
}
