import {marked} from 'marked'
import {useEffect, useMemo, useRef, useState} from 'react'

import {repoUrl} from './content'
import {docBySlug, docGroups, docs, type DocMeta} from './docs'
import {highlightBlocks} from './highlight'
import {resolveDocLink, slugifyHeading, statusOf} from './markdown'
import type {Theme} from './preferences'

// DocsSidebar lists every guide grouped by section, highlighting the active doc.
function DocsSidebar({activeSlug}: {activeSlug?: string}) {
  return (
    <nav className="docs-sidebar" aria-label="Documentation">
      <a className="docs-sidebar-home" href="#/docs">
        All documentation
      </a>
      {docGroups().map((group) => (
        <div className="docs-sidebar-group" key={group.name}>
          <p className="docs-sidebar-heading">{group.name}</p>
          <ul>
            {group.docs.map((doc) => (
              <li key={doc.slug}>
                <a
                  className={doc.slug === activeSlug ? 'is-active' : undefined}
                  aria-current={doc.slug === activeSlug ? 'page' : undefined}
                  href={`#/docs/${doc.slug}`}
                >
                  {doc.title}
                </a>
              </li>
            ))}
          </ul>
        </div>
      ))}
    </nav>
  )
}

function DocsShell({activeSlug, toc, children}: {activeSlug?: string; toc?: React.ReactNode; children: React.ReactNode}) {
  return (
    <section className="band docs-band">
      <div className={toc ? 'docs-layout has-toc' : 'docs-layout'}>
        <DocsSidebar activeSlug={activeSlug} />
        <div className="docs-main">{children}</div>
        {toc}
      </div>
    </section>
  )
}

// DocsIndex is the documentation landing route: every guide as a linked card with its blurb.
export function DocsIndex() {
  return (
    <DocsShell>
      <div className="section-heading">
        <p className="eyebrow">Documentation</p>
        <h2>Guides, references, and architecture decisions.</h2>
      </div>
      {docGroups().map((group) => (
        <div className="docs-index-group" key={group.name}>
          <h3>{group.name}</h3>
          <div className="docs-card-grid">
            {group.docs.map((doc) => (
              <a className="docs-card" key={doc.slug} href={`#/docs/${doc.slug}`}>
                <h4>{doc.title}</h4>
                {doc.summary ? <p>{doc.summary}</p> : null}
              </a>
            ))}
          </div>
        </div>
      ))}
    </DocsShell>
  )
}

interface TocEntry {
  id: string
  text: string
}

// decorateTables marks wide tables for the scrolling frame and turns the feature matrix's status
// emoji into labeled pills. The Markdown stays the source of truth; this only changes the look.
function decorateTables(container: HTMLElement) {
  container.querySelectorAll('table').forEach((table) => {
    if (table.parentElement?.classList.contains('table-frame')) {
      return
    }

    const frame = document.createElement('div')
    frame.className = 'table-frame'
    table.replaceWith(frame)
    frame.appendChild(table)

    const headers = Array.from(table.querySelectorAll('th')).map((th) => th.textContent?.trim() ?? '')
    if (!headers.includes('Go') || !headers.includes('Rust')) {
      return
    }

    // Only the language columns hold statuses; a dash elsewhere (the Spec column) means "none".
    const statusColumns = new Set(['Go', 'Rust', 'TS'])

    table.classList.add('matrix')
    table.querySelectorAll('tr').forEach((row) => {
      Array.from(row.querySelectorAll('td')).forEach((cell, index) => {
        if (statusColumns.has(headers[index])) {
          decorateStatus(cell)
        }
      })
    })
  })
}

function decorateStatus(cell: HTMLTableCellElement) {
  const status = statusOf(cell.textContent ?? '')
  if (!status || cell.querySelector('.status')) {
    return
  }

  const pill = document.createElement('span')
  pill.className = `status status-${status.kind}`
  pill.textContent = status.label
  pill.title = status.label

  // Drop the leading emoji text, keep any explanation after it.
  const first = cell.firstChild
  if (first?.nodeType === Node.TEXT_NODE) {
    first.textContent = (first.textContent ?? '').replace(status.symbol, '').replace(/^\s+/, '')
  }

  cell.prepend(pill)
  if (cell.textContent !== status.label) {
    cell.classList.add('has-note')
  }
}

// enhanceDoc applies in-place enhancements to the rendered Markdown: stable heading ids, link
// rewriting (external/route/in-page), syntax colors, table decoration, and mermaid rendering for
// fenced ```mermaid blocks. Returns a cleanup that cancels any in-flight async mermaid work when
// the doc unmounts or changes.
function enhanceDoc(container: HTMLElement, doc: DocMeta, theme: Theme): () => void {
  container.querySelectorAll('h1, h2, h3, h4, h5, h6').forEach((heading) => {
    if (!heading.id && heading.textContent) {
      heading.id = slugifyHeading(heading.textContent)
    }
  })

  container.querySelectorAll<HTMLAnchorElement>('a[href]').forEach((anchor) => {
    // Resolve from the original href stashed on first pass, so re-running this enhancement (React
    // StrictMode double-invokes effects in dev) never re-parses an already-rewritten href.
    let original = anchor.getAttribute('data-doc-href')
    if (original === null) {
      original = anchor.getAttribute('href') ?? ''
      anchor.setAttribute('data-doc-href', original)
    }

    const link = resolveDocLink(original, doc.slug)

    if (link.kind === 'external') {
      anchor.setAttribute('target', '_blank')
      anchor.setAttribute('rel', 'noopener noreferrer')
    } else if (link.kind === 'route') {
      anchor.setAttribute('href', `#/docs/${link.slug}`)
    } else if (link.kind === 'anchor') {
      // Keep the doc route stable; scroll within the page instead of replacing the hash route.
      anchor.setAttribute('href', `#/docs/${doc.slug}`)
      if (anchor.dataset.scrollBound !== '1') {
        anchor.dataset.scrollBound = '1'
        anchor.addEventListener('click', (event) => {
          event.preventDefault()
          container.querySelector(`#${CSS.escape(link.id)}`)?.scrollIntoView({behavior: 'smooth'})
        })
      }
    } else if (link.kind === 'repo') {
      anchor.setAttribute('href', `${repoUrl}/blob/main/${link.path}`)
      anchor.setAttribute('target', '_blank')
      anchor.setAttribute('rel', 'noopener noreferrer')
    }
  })

  highlightBlocks(container)
  decorateTables(container)

  // Mermaid replaces each block with an SVG, so keep the source to re-render on a theme change.
  container.querySelectorAll('code.language-mermaid').forEach((code) => {
    const block = document.createElement('div')
    block.className = 'mermaid'
    block.dataset.source = code.textContent ?? ''
    code.parentElement?.replaceWith(block)
  })

  const mermaidBlocks = Array.from(container.querySelectorAll<HTMLElement>('div.mermaid'))
  if (mermaidBlocks.length === 0) {
    return () => {}
  }

  let cancelled = false
  void (async () => {
    const {default: mermaid} = await import('mermaid')
    if (cancelled) {
      return
    }

    mermaid.initialize({startOnLoad: false, theme: theme === 'dark' ? 'dark' : 'neutral'})

    for (const block of mermaidBlocks) {
      block.removeAttribute('data-processed')
      block.textContent = block.dataset.source ?? ''
    }

    await mermaid.run({nodes: mermaidBlocks})
  })()

  return () => {
    cancelled = true
  }
}

function useActiveHeading(ids: string[]): string | undefined {
  const [active, setActive] = useState<string>()

  useEffect(() => {
    if (ids.length === 0 || typeof IntersectionObserver === 'undefined') {
      return
    }

    const observer = new IntersectionObserver(
      (entries) => {
        const visible = entries.filter((entry) => entry.isIntersecting)
        if (visible.length > 0) {
          setActive(visible[0].target.id)
        }
      },
      {rootMargin: '-96px 0px -70% 0px'},
    )

    ids.forEach((id) => {
      const heading = document.getElementById(id)
      if (heading) {
        observer.observe(heading)
      }
    })

    return () => observer.disconnect()
  }, [ids])

  return active
}

function DocToc({entries, slug}: {entries: TocEntry[]; slug: string}) {
  const active = useActiveHeading(useMemo(() => entries.map((entry) => entry.id), [entries]))

  return (
    <nav className="doc-toc" aria-label="On this page">
      <p className="docs-sidebar-heading">On this page</p>
      <ul>
        {entries.map((entry) => (
          <li key={entry.id}>
            <a
              className={entry.id === active ? 'is-active' : undefined}
              href={`#/docs/${slug}`}
              onClick={(event) => {
                event.preventDefault()
                document.getElementById(entry.id)?.scrollIntoView({behavior: 'smooth'})
              }}
            >
              {entry.text}
            </a>
          </li>
        ))}
      </ul>
    </nav>
  )
}

function DocPager({doc}: {doc: DocMeta}) {
  const index = docs.findIndex((candidate) => candidate.slug === doc.slug)
  const previous = index > 0 ? docs[index - 1] : undefined
  const next = index >= 0 && index < docs.length - 1 ? docs[index + 1] : undefined

  return (
    <footer className="doc-footer">
      <a className="doc-edit" href={`${repoUrl}/edit/main/docs/${doc.slug}.md`} target="_blank" rel="noopener noreferrer">
        Edit this page on GitHub ↗
      </a>
      <div className="doc-pager">
        {previous ? (
          <a className="doc-pager-link" href={`#/docs/${previous.slug}`}>
            <span>← Previous</span>
            <strong>{previous.title}</strong>
          </a>
        ) : (
          <span />
        )}
        {next ? (
          <a className="doc-pager-link is-next" href={`#/docs/${next.slug}`}>
            <span>Next →</span>
            <strong>{next.title}</strong>
          </a>
        ) : null}
      </div>
    </footer>
  )
}

// DocPage renders a single guide from its bundled Markdown.
export function DocPage({slug, theme}: {slug: string; theme: Theme}) {
  const doc = docBySlug(slug)
  const contentRef = useRef<HTMLDivElement>(null)
  const [toc, setToc] = useState<TocEntry[]>([])

  const html = useMemo(
    () => (doc ? (marked.parse(doc.markdown, {async: false}) as string) : ''),
    [doc],
  )

  useEffect(() => {
    const container = contentRef.current
    if (!container || !doc) {
      return
    }

    const cleanup = enhanceDoc(container, doc, theme)
    setToc(
      Array.from(container.querySelectorAll('h2'))
        .filter((heading) => heading.id)
        .map((heading) => ({id: heading.id, text: heading.textContent ?? ''})),
    )

    return cleanup
  }, [doc, html, theme])

  if (!doc) {
    return (
      <DocsShell>
        <div className="doc-missing">
          <h2>Page not found</h2>
          <p>
            No document matches <code>{slug}</code>.{' '}
            <a href="#/docs">Back to documentation</a>.
          </p>
        </div>
      </DocsShell>
    )
  }

  return (
    <DocsShell activeSlug={doc.slug} toc={toc.length > 2 ? <DocToc entries={toc} slug={doc.slug} /> : undefined}>
      <article className="doc-article">
        <p className="eyebrow">{doc.group}</p>
        <div className="doc-content" ref={contentRef} dangerouslySetInnerHTML={{__html: html}} />
        <DocPager doc={doc} />
      </article>
    </DocsShell>
  )
}
