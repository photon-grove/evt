import {useEffect, useMemo, useState} from 'react'

import {DiagramViewer} from '@photon-grove/react-flow-diagrams'

import {EventLogArt, EvtMark, FeatureGlyph} from './ClipArt'
import {
  backends,
  capabilities,
  type ContentCard,
  cookbook,
  examples,
  installCommand,
  type Lang,
  langs,
  packages,
  quickStart,
  repoUrl,
  tour,
} from './content'
import {diagrams} from './diagrams'
import {docBySlug} from './docs'
import {DocPage, DocsIndex} from './DocsView'
import {highlightCode} from './highlight'
import {type Theme, useLang, useTheme} from './preferences'
import {photonGroveUrl} from './siteConfig'
import {useHashRoute} from './useHashRoute'

function CopyButton({value, label}: {value: string; label: string}) {
  const [copied, setCopied] = useState(false)

  const onCopy = () => {
    void navigator.clipboard?.writeText(value).then(() => {
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1600)
    })
  }

  return (
    <button type="button" className="copy" onClick={onCopy} aria-label={label}>
      {copied ? 'Copied' : 'Copy'}
    </button>
  )
}

function LangSwitch({lang, onChange, label}: {lang: Lang; onChange: (lang: Lang) => void; label: string}) {
  return (
    <div className="lang-switch" role="group" aria-label={label}>
      {langs.map((option) => (
        <button
          type="button"
          key={option.id}
          className={option.id === lang ? 'is-active' : undefined}
          aria-pressed={option.id === lang}
          onClick={() => onChange(option.id)}
        >
          {option.label}
        </button>
      ))}
    </div>
  )
}

// HighlightedCode renders a code sample with syntax colors. highlight.js escapes the source, so
// the generated markup is safe to inject.
function HighlightedCode({code, language}: {code: string; language: string}) {
  const html = useMemo(() => highlightCode(code, language), [code, language])

  if (html === undefined) {
    return <code>{code}</code>
  }

  return <code className="hljs" dangerouslySetInnerHTML={{__html: html}} />
}

function CodeCard({file, code, language, children}: {file: string; code: string; language: string; children?: React.ReactNode}) {
  return (
    <figure className="code-card">
      <figcaption className="code-head">
        <span className="dots" aria-hidden="true">
          <i />
          <i />
          <i />
        </span>
        <span className="code-file">{file}</span>
        {children}
        <CopyButton value={code} label={`Copy ${file}`} />
      </figcaption>
      <pre>
        <HighlightedCode code={code} language={language} />
      </pre>
    </figure>
  )
}

function CardLink({doc, label = 'Read the guide'}: {doc?: string; label?: string}) {
  if (!doc) {
    return null
  }

  return (
    <a className="card-link" href={`#/docs/${doc}`}>
      {label} →
    </a>
  )
}

function FeatureCard({item}: {item: ContentCard}) {
  return (
    <article className="feature-card">
      <div className="feature-top">
        {item.icon ? (
          <span className="feature-icon">
            <FeatureGlyph icon={item.icon} />
          </span>
        ) : null}
        {item.tag ? <span className="tag">{item.tag}</span> : null}
      </div>
      <h3>{item.title}</h3>
      <p>{item.body}</p>
      <CardLink doc={item.doc} />
    </article>
  )
}

function ThemeToggle({theme, onToggle}: {theme: Theme; onToggle: () => void}) {
  const next = theme === 'dark' ? 'light' : 'dark'

  return (
    <button type="button" className="theme-toggle" onClick={onToggle} aria-label={`Switch to ${next} theme`} title={`Switch to ${next} theme`}>
      <svg viewBox="0 0 24 24" aria-hidden="true">
        {theme === 'dark' ? (
          <>
            <circle cx="12" cy="12" r="4" />
            <path d="M12 2.5v2M12 19.5v2M2.5 12h2M19.5 12h2M5.3 5.3l1.4 1.4M17.3 17.3l1.4 1.4M5.3 18.7l1.4-1.4M17.3 6.7l1.4-1.4" />
          </>
        ) : (
          <path d="M20 14.5A8 8 0 0 1 9.5 4a8 8 0 1 0 10.5 10.5z" />
        )}
      </svg>
    </button>
  )
}

function Brand() {
  return (
    <a className="brand" href="#/" aria-label="evt home">
      <EvtMark size={34} />
      <span className="brand-word">evt</span>
      <span className="brand-text">Event sourcing for Go and Rust</span>
    </a>
  )
}

function Nav({theme, onToggleTheme}: {theme: Theme; onToggleTheme: () => void}) {
  return (
    <header className="nav">
      <Brand />
      <nav aria-label="Primary navigation">
        <a href="#start">Quick start</a>
        <a href="#concepts">Concepts</a>
        <a href="#features">Features</a>
        <a href="#examples">Examples</a>
        <a href="#/docs">Docs</a>
        <ThemeToggle theme={theme} onToggle={onToggleTheme} />
        <a className="nav-cta" href={repoUrl}>
          GitHub ↗
        </a>
      </nav>
    </header>
  )
}

function ConceptTour({lang, onLang}: {lang: Lang; onLang: (lang: Lang) => void}) {
  const [active, setActive] = useState(0)
  const stop = tour[active]

  return (
    <section className="band tour-band" id="concepts">
      <div className="section-heading">
        <p className="eyebrow">Concepts tour</p>
        <h2>From a command to a view, in five stops.</h2>
        <p className="section-sub">
          Every piece is a small contract you implement or configure. Pick a stop to see the code for
          it.
        </p>
      </div>

      <ol className="tour-rail" role="tablist" aria-label="Concepts">
        {tour.map((item, index) => (
          <li key={item.id}>
            <button
              type="button"
              role="tab"
              id={`tour-tab-${item.id}`}
              aria-selected={index === active}
              aria-controls="tour-panel"
              className={index === active ? 'is-active' : index < active ? 'is-past' : undefined}
              onClick={() => setActive(index)}
            >
              <span className="tour-index">{index + 1}</span>
              <span className="tour-title">{item.title}</span>
              <span className="tour-tagline">{item.tagline}</span>
            </button>
          </li>
        ))}
      </ol>

      <div className="tour-panel" id="tour-panel" role="tabpanel" aria-labelledby={`tour-tab-${stop.id}`}>
        <div className="tour-copy">
          <p className="tour-step">
            Stop {active + 1} of {tour.length}
          </p>
          <h3>{stop.tagline}</h3>
          <p>{stop.body}</p>
          <CardLink doc={stop.doc} />
          <div className="tour-nav">
            <button type="button" className="button secondary small" disabled={active === 0} onClick={() => setActive(active - 1)}>
              ← Back
            </button>
            <button
              type="button"
              className="button primary small"
              disabled={active === tour.length - 1}
              onClick={() => setActive(active + 1)}
            >
              Next stop →
            </button>
          </div>
        </div>
        <CodeCard file={`${stop.id}.${lang === 'go' ? 'go' : 'rs'}`} code={stop.code[lang]} language={lang}>
          <LangSwitch lang={lang} onChange={onLang} label="Code language" />
        </CodeCard>
      </div>
    </section>
  )
}

function Home({lang, onLang}: {lang: Lang; onLang: (lang: Lang) => void}) {
  const sample = quickStart[lang]

  return (
    <>
      <section className="hero" id="top">
        <div className="hero-copy">
          <p className="eyebrow">Event sourcing for Go and Rust</p>
          <h1>
            Immutable events as truth. <span className="accent">Views you can rebuild.</span>
          </h1>
          <p className="lede">
            evt is a library for event-sourced services. Aggregates handle commands, the events they
            decide on go into an append-only log in <strong>DynamoDB</strong> or{' '}
            <strong>PostgreSQL</strong>, and views are projections you can wipe and rebuild from that
            log. Go and Rust read and write the same bytes.
          </p>
          <div className="install">
            <LangSwitch lang={lang} onChange={onLang} label="Install language" />
            <span className="prompt">$</span>
            <code>{installCommand[lang]}</code>
            <CopyButton value={installCommand[lang]} label="Copy install command" />
          </div>
          <div className="hero-actions">
            <a className="button primary" href="#start">
              Quick start
            </a>
            <a className="button secondary" href="#concepts">
              Take the concepts tour
            </a>
          </div>
          <ul className="hero-facts" aria-label="At a glance">
            <li>Apache-2.0</li>
            <li>Go module · Rust crates</li>
            <li>No runtime, no base types</li>
          </ul>
        </div>
        <div className="hero-art" aria-hidden="true">
          <EventLogArt />
        </div>
      </section>

      <section className="band backends-band" aria-label="Storage backends">
        <div className="backend-grid">
          {backends.map((backend) => (
            <a className="backend" key={backend.name} href={`#/docs/${backend.doc}`}>
              <span className="backend-name">{backend.name}</span>
              <span className="backend-body">{backend.body}</span>
              <code className="backend-detail">{backend.detail}</code>
            </a>
          ))}
        </div>
      </section>

      <section className="band split-band" id="start">
        <div className="split-copy">
          <p className="eyebrow">Quick start</p>
          <h2>One aggregate, in memory first.</h2>
          <p className="section-sub">
            The in-memory store and the durable stores take the same aggregate, so you test the
            domain logic in memory and run it on DynamoDB or PostgreSQL without changing it.
          </p>
          <ol className="steps">
            <li>Model one aggregate with its command and event types.</li>
            <li>Test it on the in-memory store, which is fast and deterministic.</li>
            <li>Point production at a DynamoDB or PostgreSQL repository.</li>
            <li>Add projectors once the event contract has settled.</li>
          </ol>
          <p className="split-more">
            <a className="card-link" href="#/docs/guide/getting-started">
              Getting started guide →
            </a>
          </p>
        </div>
        <CodeCard file={sample.file} code={sample.code} language={lang}>
          <LangSwitch lang={lang} onChange={onLang} label="Code language" />
        </CodeCard>
      </section>

      <ConceptTour lang={lang} onLang={onLang} />

      <section className="band intro-band" id="features">
        <div className="section-heading">
          <p className="eyebrow">Features</p>
          <h2>What you get out of the box.</h2>
          <p className="section-lead">
            The <a href="#/docs/reference/feature-matrix">feature matrix</a> shows what each language
            implements today.
          </p>
        </div>
        <div className="capability-grid">
          {capabilities.map((item) => (
            <FeatureCard item={item} key={item.title} />
          ))}
        </div>
      </section>

      <section className="band diagram-section" id="diagrams">
        <div className="section-heading">
          <p className="eyebrow">Interactive architecture</p>
          <h2>Follow a command through to its projections and the stream.</h2>
        </div>
        <div className="diagram-frame">
          <DiagramViewer diagrams={diagrams} title="evt" subtitle="Architecture guide" />
        </div>
      </section>

      <section className="band examples-band" id="examples">
        <div className="section-heading">
          <p className="eyebrow">Examples</p>
          <h2>Runnable, and run in CI.</h2>
          <p className="section-sub">Each example builds and passes its tests on every pull request.</p>
        </div>
        <div className="examples">
          {examples.map((item) => (
            <article className="example" key={`${item.lang}-${item.title}`}>
              <div className="feature-top">
                <h3>{item.title}</h3>
                <span className={`tag tag-${item.lang.toLowerCase()}`}>{item.lang}</span>
              </div>
              <p>{item.body}</p>
              <div className="example-command">
                <code>{item.command}</code>
                <CopyButton value={item.command} label={`Copy the ${item.title} command`} />
              </div>
              <a className="card-link" href={item.href}>
                View source ↗
              </a>
            </article>
          ))}
        </div>
      </section>

      <section className="band packages-band" id="packages">
        <div className="section-heading packages-heading">
          <div>
            <p className="eyebrow">Package reference</p>
            <h2>Import only what you use.</h2>
          </div>
          <LangSwitch lang={lang} onChange={onLang} label="Package language" />
        </div>
        <ul className="package-list">
          {packages[lang].map((pkg) => (
            <li key={pkg.name}>
              <a className="package" href={pkg.href}>
                <code className="package-name">{pkg.name}</code>
                <p>{pkg.body}</p>
              </a>
            </li>
          ))}
        </ul>
      </section>

      <section className="band cookbook-band" id="cookbook">
        <div className="section-heading">
          <p className="eyebrow">Integration cookbook</p>
          <h2>Patterns for production.</h2>
        </div>
        <div className="cookbook-grid">
          {cookbook.map((item) => (
            <article className="recipe" key={item.title}>
              <h3>{item.title}</h3>
              <p>{item.body}</p>
              <CardLink doc={item.doc} />
            </article>
          ))}
        </div>
      </section>
    </>
  )
}

export function App() {
  const route = useHashRoute()
  const [theme, toggleTheme] = useTheme()
  const [lang, setLang] = useLang()

  // Scroll handling on navigation: doc routes start at the top; the home route honors a plain
  // "#section" fragment (so cross-page nav like Quick start/Architecture still scrolls once home
  // mounts).
  useEffect(() => {
    if (route.name !== 'home') {
      window.scrollTo(0, 0)

      return
    }

    const fragment = window.location.hash.replace(/^#/, '')
    if (fragment && !fragment.startsWith('/')) {
      requestAnimationFrame(() => document.getElementById(fragment)?.scrollIntoView())
    }
  }, [route])

  return (
    <main>
      <Nav theme={theme} onToggleTheme={toggleTheme} />

      {route.name === 'home' && <Home lang={lang} onLang={setLang} />}
      {route.name === 'docs-index' && <DocsIndex />}
      {route.name === 'doc' && route.slug ? <DocPage slug={route.slug} theme={theme} /> : null}

      <footer className="site-footer">
        <div className="footer-inner">
          <Brand />
          <nav aria-label="Footer navigation">
            <a href="#/docs">Docs</a>
            {docBySlug('guide/examples') ? <a href="#/docs/guide/examples">Examples</a> : null}
            <a href="#/docs/reference/feature-matrix">Feature matrix</a>
            <a href="https://pkg.go.dev/github.com/photon-grove/evt/go">pkg.go.dev ↗</a>
            <a href="https://docs.rs/photon-evt">docs.rs ↗</a>
            <a href={repoUrl}>GitHub ↗</a>
          </nav>
        </div>
        <p className="attribution">
          Apache-2.0 · Built by <a href={photonGroveUrl}>Photon Grove</a>, a Colorado software studio.
        </p>
      </footer>
    </main>
  )
}
