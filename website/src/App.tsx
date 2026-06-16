import {useState} from 'react'

import {DiagramViewer} from '@photon-grove/react-flow-diagrams'

import {EventLogArt} from './ClipArt'
import {
  capabilities,
  cookbook,
  docLinks,
  examples,
  gettingStarted,
  installCommand,
  packages,
  quickStartCode,
  repoUrl,
} from './content'
import {diagrams} from './diagrams'
import {photonGroveUrl} from './siteConfig'

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

export function App() {
  return (
    <main>
      <header className="nav">
        <a className="brand" href="#top" aria-label="evt home">
          <span className="brand-mark">evt</span>
          <span className="brand-text">Event sourcing for Go</span>
        </a>
        <nav aria-label="Primary navigation">
          <a href="#start">Quick start</a>
          <a href="#packages">Packages</a>
          <a href="#diagrams">Architecture</a>
          <a href="#docs">Docs</a>
          <a className="nav-cta" href={repoUrl}>
            GitHub ↗
          </a>
        </nav>
      </header>

      <section className="hero" id="top">
        <div className="hero-copy">
          <p className="eyebrow">A Go event-sourcing framework</p>
          <h1>
            Immutable events as truth. <span className="accent">Views you can rebuild.</span>
          </h1>
          <p className="lede">
            <strong>evt</strong> is a compact Go framework for event-sourced services: aggregate
            commands, append-only event logs, snapshots, rebuildable projections, DynamoDB Streams
            projectors, and publisher helpers — testable from the first line you write.
          </p>
          <div className="install">
            <span className="prompt">$</span>
            <code>{installCommand}</code>
            <CopyButton value={installCommand} label="Copy install command" />
          </div>
          <div className="hero-actions">
            <a className="button primary" href="#start">
              Quick start
            </a>
            <a className="button secondary" href="#diagrams">
              Explore the architecture
            </a>
          </div>
        </div>
        <div className="hero-art" aria-hidden="true">
          <EventLogArt />
        </div>
      </section>

      <section className="band intro-band">
        <div className="section-heading">
          <p className="eyebrow">Framework surface</p>
          <h2>Everything an event-sourced Go service needs — and nothing it doesn&rsquo;t.</h2>
          <p className="section-sub">
            Small, explicit packages with stable contracts. Adopt the pieces you need; the rest stay
            out of your way.
          </p>
        </div>
        <div className="capability-grid">
          {capabilities.map((item) => (
            <article className="feature-card" key={`${item.tag}-${item.title}`}>
              <code className="card-tag">{item.tag}</code>
              <h3>{item.title}</h3>
              <p>{item.body}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="band split-band" id="start">
        <div className="split-copy">
          <p className="eyebrow">First path</p>
          <h2>Test in memory, ship to DynamoDB — same model.</h2>
          <p className="section-sub">
            Your aggregates never learn which store they run against. Prove behavior with fast
            in-memory tests, then move production writes over without rewriting domain code.
          </p>
          <ol className="steps">
            {gettingStarted.map((step) => (
              <li key={step}>{step}</li>
            ))}
          </ol>
        </div>
        <figure className="code-card">
          <figcaption className="code-head">
            <span className="dots" aria-hidden="true">
              <i />
              <i />
              <i />
            </span>
            <span className="code-file">account_test.go</span>
            <CopyButton value={quickStartCode} label="Copy quick start example" />
          </figcaption>
          <pre>
            <code>{quickStartCode}</code>
          </pre>
        </figure>
      </section>

      <section className="band diagram-section" id="diagrams">
        <div className="section-heading">
          <p className="eyebrow">Interactive architecture</p>
          <h2>Trace the runtime from command to projection and async fanout.</h2>
        </div>
        <div className="diagram-frame">
          <DiagramViewer diagrams={diagrams} title="evt" subtitle="Architecture guide" />
        </div>
      </section>

      <section className="band packages-band" id="packages">
        <div className="section-heading">
          <p className="eyebrow">Package reference</p>
          <h2>Import only what the service uses.</h2>
          <p className="section-sub">
            Every package lives under <code>github.com/photon-grove/evt</code>.
          </p>
        </div>
        <ul className="package-list">
          {packages.map((pkg) => (
            <li className="package" key={pkg.name}>
              <code className="package-name">{pkg.name}</code>
              <p>{pkg.body}</p>
            </li>
          ))}
        </ul>
      </section>

      <section className="band cookbook-band">
        <div className="section-heading">
          <p className="eyebrow">Integration cookbook</p>
          <h2>Patterns worth copying straight into an adopter project.</h2>
        </div>
        <div className="cookbook-grid">
          {cookbook.map((item) => (
            <article className="recipe" key={item.title}>
              <h3>{item.title}</h3>
              <p>{item.body}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="band docs-band" id="docs">
        <div className="section-heading">
          <p className="eyebrow">Documentation</p>
          <h2>Read the source of truth.</h2>
          <p className="section-sub">
            Concept guides, integration notes, and the invariants that keep stored events readable.
          </p>
        </div>
        <div className="docs-grid">
          {docLinks.map((doc) => (
            <a className="doc-card" key={doc.title} href={doc.href}>
              <span className="doc-arrow" aria-hidden="true">
                →
              </span>
              <h3>{doc.title}</h3>
              <p>{doc.body}</p>
            </a>
          ))}
        </div>
      </section>

      <section className="band examples-band">
        <div className="section-heading">
          <p className="eyebrow">Run it locally</p>
          <h2>Concrete entry points for adoption.</h2>
        </div>
        <div className="examples">
          {examples.map((item) => (
            <article className="example" key={item.title}>
              <h3>{item.title}</h3>
              <p>{item.body}</p>
              <code>{item.command}</code>
            </article>
          ))}
        </div>
      </section>

      <footer className="site-footer">
        <div className="footer-inner">
          <a className="brand" href="#top" aria-label="evt home">
            <span className="brand-mark">evt</span>
            <span className="brand-text">Event sourcing for Go</span>
          </a>
          <nav aria-label="Footer navigation">
            <a href={`${repoUrl}#install`}>Install</a>
            <a href="#start">Quick start</a>
            <a href="#docs">Docs</a>
            <a href={repoUrl}>GitHub ↗</a>
          </nav>
        </div>
        <p className="attribution">
          Apache-2.0 · Built with care by <a href={photonGroveUrl}>Photon Grove</a>, a Colorado
          software studio.
        </p>
      </footer>
    </main>
  )
}
