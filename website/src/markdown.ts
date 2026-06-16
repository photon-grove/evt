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
  | {kind: 'asis'; href: string}

// resolveDocLink decides how an href inside the doc identified by fromSlug should behave:
//   - absolute/protocol URLs open externally,
//   - pure "#fragment" links scroll within the current doc,
//   - relative links to another ".md" become an in-site doc route (resolved against fromSlug's dir),
//   - everything else is left untouched.
export function resolveDocLink(href: string, fromSlug: string): DocLink {
  if (/^[a-z][a-z0-9+.-]*:/i.test(href) || href.startsWith('//')) {
    return {kind: 'external', href}
  }

  if (href.startsWith('#')) {
    return {kind: 'anchor', id: href.slice(1)}
  }

  const [pathPart] = href.split('#')
  if (pathPart.endsWith('.md')) {
    return {kind: 'route', slug: resolveRelativePath(fromSlug, pathPart).replace(/\.md$/, '')}
  }

  return {kind: 'asis', href}
}

// resolveRelativePath resolves target (a relative path like "adr/x.md" or "../concepts.md") against
// the directory of fromSlug, returning a docs-root-relative path.
function resolveRelativePath(fromSlug: string, target: string): string {
  const baseDir = fromSlug.includes('/') ? fromSlug.slice(0, fromSlug.lastIndexOf('/')) : ''
  const parts = baseDir ? baseDir.split('/') : []

  for (const segment of target.split('/')) {
    if (segment === '' || segment === '.') {
      continue
    }

    if (segment === '..') {
      parts.pop()
    } else {
      parts.push(segment)
    }
  }

  return parts.join('/')
}
