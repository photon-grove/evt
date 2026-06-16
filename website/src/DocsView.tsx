import {marked} from 'marked'
import {useEffect, useMemo, useRef} from 'react'

import {docBySlug, docGroups, type DocMeta} from './docs'
import {resolveDocLink, slugifyHeading} from './markdown'

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

function DocsShell({activeSlug, children}: {activeSlug?: string; children: React.ReactNode}) {
  return (
    <section className="band docs-band">
      <div className="docs-layout">
        <DocsSidebar activeSlug={activeSlug} />
        <div className="docs-main">{children}</div>
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

// renderDoc applies in-place enhancements to the rendered Markdown: stable heading ids, link
// rewriting (external/route/in-page), and mermaid rendering for fenced ```mermaid blocks. Returns a
// cleanup that cancels any in-flight async mermaid work when the doc unmounts or changes.
function enhanceDoc(container: HTMLElement, doc: DocMeta): () => void {
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
    }
  })

  const mermaidBlocks = Array.from(container.querySelectorAll('code.language-mermaid'))
  if (mermaidBlocks.length === 0) {
    return () => {}
  }

  let cancelled = false
  void (async () => {
    const {default: mermaid} = await import('mermaid')
    if (cancelled) {
      return
    }

    mermaid.initialize({startOnLoad: false, theme: 'neutral'})

    for (const code of mermaidBlocks) {
      const block = document.createElement('div')
      block.className = 'mermaid'
      block.textContent = code.textContent ?? ''
      code.parentElement?.replaceWith(block)
    }

    const nodes = Array.from(container.querySelectorAll<HTMLElement>('div.mermaid'))
    if (nodes.length > 0) {
      await mermaid.run({nodes})
    }
  })()

  return () => {
    cancelled = true
  }
}

// DocPage renders a single guide from its bundled Markdown.
export function DocPage({slug}: {slug: string}) {
  const doc = docBySlug(slug)
  const contentRef = useRef<HTMLDivElement>(null)

  const html = useMemo(
    () => (doc ? (marked.parse(doc.markdown, {async: false}) as string) : ''),
    [doc],
  )

  useEffect(() => {
    const container = contentRef.current
    if (!container || !doc) {
      return
    }

    return enhanceDoc(container, doc)
  }, [doc, html])

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
    <DocsShell activeSlug={doc.slug}>
      <article className="doc-article">
        <p className="eyebrow">{doc.group}</p>
        <div className="doc-content" ref={contentRef} dangerouslySetInnerHTML={{__html: html}} />
      </article>
    </DocsShell>
  )
}
