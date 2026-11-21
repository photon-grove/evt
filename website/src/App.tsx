import {useEffect, useMemo, useState} from 'react'

import {DiagramViewer} from '@photon-grove/react-flow-diagrams'

import {EventLogArt, EvtMark, FeatureGlyph} from './ClipArt'
import {
  backends,
  capabilities,
  type ContentCard,
  cookbook,
  examples,
  fixtures,
  implementations,
  installCommand,
  type Lang,
  langExt,
  langs,
  npmUrl,
  packages,
  quickStart,
  repoUrl,
  tour,
} from './content'
import {diagrams} from './diagrams'
import {docBySlug} from './docs'
import {DocPage, DocsIndex} from './DocsView'
import {highlightCode} from './highlight'
import type {Status} from './markdown'
import {siteParityRows} from './parity'
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
          title={option.label}
          onClick={() => onChange(option.id)}
        >
          {option.short}
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

// PendingCard stands in for a code sample in a language that doesn't have the feature yet.
function PendingCard({file, note, children}: {file: string; note: string; children?: React.ReactNode}) {
  return (
    <figure className="code-card is-pending">
      <figcaption className="code-head">
        <span className="dots" aria-hidden="true">
          <i />
          <i />
          <i />
        </span>
        <span className="code-file">{file}</span>
        {children}
      </figcaption>
      <div className="pending-body">
        <span className="pending-badge">Not in this language yet</span>
        <p>{note}</p>
        <a className="card-link" href="#/docs/reference/feature-matrix">
          Feature matrix →
        </a>
      </div>
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
      <span className="brand-text">Event sourcing for Rust, Go and TypeScript</span>
    </a>
  )
}

function Nav({theme, onToggleTheme}: {theme: Theme; onToggleTheme: () => void}) {
  return (
    <header className="nav">
      <Brand />
      <nav className="nav-links" aria-label="Primary navigation">
        <a href="#start">Quick start</a>
        <a href="#concepts">Concepts</a>
        <a href="#languages">Languages</a>
        <a href="#features">Features</a>
        <a href="#status">Status</a>
        <a href="#/docs">Docs</a>
      </nav>
      <div className="nav-actions">
        <ThemeToggle theme={theme} onToggle={onToggleTheme} />
        <a className="nav-cta" href={repoUrl}>
          GitHub ↗
        </a>
      </div>
    </header>
  )
}

function TourCode({stop, lang, onLang}: {stop: (typeof tour)[number]; lang: Lang; onLang: (lang: Lang) => void}) {
  const file = `${stop.id}.${langExt(lang)}`
  const code = stop.code[lang]
  const switcher = <LangSwitch lang={lang} onChange={onLang} label="Code language" />

  if (code === undefined) {
    return (
      <PendingCard file={file} note={stop.pending?.[lang] ?? 'This piece has not reached this language yet.'}>
        {switcher}
      </PendingCard>
    )
  }

  return (
    <CodeCard file={file} code={code} language={lang}>
      {switcher}
    </CodeCard>
  )
}

function StatusPill({status}: {status: Status | undefined}) {
  if (!status) {
    return <span className="status status-na">Unknown</span>
  }

  return (
    <span className={`status status-${status.kind}`} title={status.label}>
      <span className="status-dot" aria-hidden="true" />
      <span className="status-text">{status.kind === 'na' ? '—' : status.label}</span>
    </span>
  )
}

const LEGEND: {kind: Status['kind']; label: string}[] = [
  {kind: 'shipped', label: 'Shipped'},
  {kind: 'progress', label: 'In progress'},
  {kind: 'planned', label: 'Planned'},
  {kind: 'deferred', label: 'Deferred'},
  {kind: 'na', label: 'Not applicable'},
]

function ParityBand() {
  const rows = useMemo(() => siteParityRows(), [])
  if (rows.length === 0) {
    return null
  }

  return (
    <section className="band parity-band" id="status">
      <div className="section-heading">
        <p className="eyebrow">Status</p>
        <h2>Where each language stands today.</h2>
        <p className="section-sub">
          This grid is read from the <a href="#/docs/reference/feature-matrix">feature matrix</a> when
          the site is built, so it shows what is on the default branch, not plans. TypeScript is being
          ported in phases, and its cells fill in as each phase lands.
        </p>
      </div>
      <ul className="parity-legend" aria-label="Legend">
        {LEGEND.map((item) => (
          <li key={item.kind}>
            <span className={`status status-${item.kind}`}>
              <span className="status-dot" aria-hidden="true" />
            </span>
            {item.label}
          </li>
        ))}
      </ul>
      <div className="parity-frame">
        <table className="parity">
          <thead>
            <tr>
              <th scope="col">Capability</th>
              {langs.map((option) => (
                <th scope="col" key={option.id}>
                  <span className="parity-lang-long">{option.label}</span>
                  <span className="parity-lang-short" aria-hidden="true">
                    {option.short}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.label}>
                <th scope="row">{row.label}</th>
                {langs.map((option) => (
                  <td key={option.id}>
                    <StatusPill status={row.statuses[option.id]} />
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="parity-more">
        <a className="card-link" href="#/docs/reference/feature-matrix">
          Every capability, with notes →
        </a>
      </p>
    </section>
  )
}

function LanguagesBand() {
  return (
    <section className="band languages-band" id="languages">
      <div className="section-heading">
        <p className="eyebrow">One format, three languages</p>
        <h2>Write a stream from Rust. Read it from Go.</h2>
        <p className="section-sub">
          evt isn't a binding. Each language is a native implementation of one normative storage
          format, and shared golden fixtures check them byte for byte. Go and Rust also take turns
          writing the same streams in live runs against DynamoDB and PostgreSQL, and the TypeScript
          driver joins those runs for the scenarios it supports.
        </p>
      </div>

      <div className="format-flow">
        <ul className="impl-grid">
          {implementations.map((impl) => (
            <li key={impl.lang}>
              <a className={`impl impl-${impl.lang}`} href={impl.href}>
                <span className="impl-top">
                  <span className={`lang-badge lang-badge-${impl.lang}`}>{langs.find((option) => option.id === impl.lang)?.label}</span>
                  <span className={`impl-status impl-status-${impl.status}`}>{impl.statusLabel}</span>
                </span>
                <code className="impl-name">{impl.name}</code>
                <span className="impl-body">{impl.body}</span>
                <span className="impl-registry">{impl.registry} ↗</span>
              </a>
            </li>
          ))}
        </ul>

        <div className="format-spine" aria-hidden="true">
          <span />
          <span />
          <span />
        </div>

        <a className="format-bar" href="#/docs/reference/storage-format">
          <span className="format-title">
            <strong>One storage format</strong>
            <span className="format-normative">normative</span>
          </span>
          <span className="format-parts">
            DynamoDB items · PostgreSQL rows · event JSON · snapshots · outcome rows · stream envelopes
          </span>
          <span className="format-link">Read the spec →</span>
        </a>

        <div className="fixture-panel">
          <p className="fixture-caption">
            <strong>compat/fixtures</strong> · Go generates most vectors and Rust its extensions'.
            TypeScript checks itself against the ones it covers so far. Nobody edits them by hand.
          </p>
          <ul className="fixture-list">
            {fixtures.map((fixture) => (
              <li key={fixture.file}>
                <a href={`${repoUrl}/blob/main/compat/fixtures/${fixture.file}`}>
                  <code>{fixture.file}</code>
                  <span className={`tag tag-${fixture.writer.toLowerCase()}`}>{fixture.writer}</span>
                </a>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </section>
  )
}

function ConceptTour({lang, onLang}: {lang: Lang; onLang: (lang: Lang) => void}) {
  const [active, setActive] = useState(0)
  const stop = tour[active]

  // Arrow keys move between stops, as in any tab list.
  const onRailKey = (event: React.KeyboardEvent) => {
    const step = event.key === 'ArrowRight' ? 1 : event.key === 'ArrowLeft' ? -1 : 0
    if (step === 0) {
      return
    }

    event.preventDefault()
    const next = (active + step + tour.length) % tour.length
    setActive(next)
    document.getElementById(`tour-tab-${tour[next].id}`)?.focus()
  }

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

      <ol className="tour-rail" role="tablist" aria-label="Concepts" onKeyDown={onRailKey}>
        {tour.map((item, index) => (
          <li key={item.id} role="presentation">
            <button
              type="button"
              role="tab"
              id={`tour-tab-${item.id}`}
              aria-selected={index === active}
              aria-controls="tour-panel"
              tabIndex={index === active ? 0 : -1}
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
        <TourCode stop={stop} lang={lang} onLang={onLang} />
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
          <p className="eyebrow">Event sourcing for Rust, Go and TypeScript</p>
          <h1>
            Immutable events as truth. <span className="accent">Views you can rebuild.</span>
          </h1>
          <p className="lede">
            evt is a library for event-sourced services. Aggregates turn commands into immutable
            events, each command commits atomically to an append-only log in{' '}
            <strong>DynamoDB</strong> or <strong>PostgreSQL</strong>, and views are derived from that
            log, so it can reproduce them. <strong>Rust, Go and TypeScript</strong> implement one
            storage format, checked byte for byte.
          </p>
          <div className="install">
            <div className="install-head">
              <LangSwitch lang={lang} onChange={onLang} label="Install language" />
              {lang === 'ts' ? <span className="install-note">being ported</span> : null}
            </div>
            <div className="install-line">
              <span className="prompt">$</span>
              <code>{installCommand[lang]}</code>
              <CopyButton value={installCommand[lang]} label="Copy install command" />
            </div>
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
            <li>Rust crate · Go module · npm package</li>
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

      <LanguagesBand />

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

      <ParityBand />

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
            <a href={npmUrl}>npm ↗</a>
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
