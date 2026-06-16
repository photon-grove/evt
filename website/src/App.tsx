import {DiagramViewer} from '@photon-grove/react-flow-diagrams'
import {useEffect} from 'react'

import {EventGarden, ToolkitShelf} from './ClipArt'
import {capabilities, type ContentCard, cookbook, examples, gettingStarted} from './content'
import {diagrams} from './diagrams'
import {DocPage, DocsIndex} from './DocsView'
import {photonGroveUrl} from './siteConfig'
import {useHashRoute} from './useHashRoute'

function Nav() {
  return (
    <header className="nav">
      <a className="brand" href="#/" aria-label="evt home">
        <span className="brand-mark">evt</span>
        <span>Event sourcing for Go</span>
      </a>
      <nav aria-label="Primary navigation">
        <a href="#/docs">Docs</a>
        <a href="#diagrams">Diagrams</a>
        <a href="#cookbook">Cookbook</a>
        <a href="https://github.com/photon-grove/evt">GitHub</a>
      </nav>
    </header>
  )
}

function CardLink({doc}: {doc?: string}) {
  if (!doc) {
    return null
  }

  return (
    <a className="card-link" href={`#/docs/${doc}`}>
      Read the guide →
    </a>
  )
}

function FeatureCard({item}: {item: ContentCard}) {
  return (
    <article className="feature-card">
      <h3>{item.title}</h3>
      <p>{item.body}</p>
      <CardLink doc={item.doc} />
    </article>
  )
}

function Home() {
  return (
    <>
      <section className="hero" id="top">
        <div className="hero-copy">
          <p className="eyebrow">Immutable events · deterministic views · DynamoDB-ready</p>
          <h1>evt</h1>
          <p className="lede">
            A compact Go framework for event-sourced systems: aggregate commands, append-only event
            logs, snapshots, rebuildable projections, DynamoDB Streams projectors, and publisher
            helpers that stay testable from day one.
          </p>
          <div className="hero-actions">
            <a className="button primary" href="#/docs">
              Read the docs
            </a>
            <a className="button secondary" href="#diagrams">
              Explore architecture
            </a>
          </div>
        </div>
        <div className="hero-art" aria-hidden="true">
          <EventGarden />
        </div>
      </section>

      <section className="band intro-band" id="docs">
        <div className="section-heading">
          <p className="eyebrow">Framework surface</p>
          <h2>Everything needed for an event-sourced Go service.</h2>
          <p className="section-lead">
            Each capability links to its guide. <a href="#/docs">Browse all documentation →</a>
          </p>
        </div>
        <div className="capability-grid">
          {capabilities.map((item) => (
            <FeatureCard item={item} key={item.title} />
          ))}
        </div>
      </section>

      <section className="band split-band">
        <div>
          <p className="eyebrow">First path</p>
          <h2>Use memory first, then move the same model to DynamoDB.</h2>
          <ol className="steps">
            {gettingStarted.map((step) => (
              <li key={step}>{step}</li>
            ))}
          </ol>
        </div>
        <ToolkitShelf />
      </section>

      <section className="diagram-section" id="diagrams">
        <div className="section-heading diagram-heading">
          <p className="eyebrow">Interactive architecture</p>
          <h2>Trace the runtime from command to projections and async fanout.</h2>
        </div>
        <div className="diagram-frame">
          <DiagramViewer diagrams={diagrams} title="evt" subtitle="Architecture guide" />
        </div>
      </section>

      <section className="band" id="cookbook">
        <div className="section-heading">
          <p className="eyebrow">Integration cookbook</p>
          <h2>Patterns worth copying directly into adopter projects.</h2>
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

      <section className="band examples-band">
        <div className="section-heading">
          <p className="eyebrow">Examples</p>
          <h2>Concrete entry points for local adoption.</h2>
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
    </>
  )
}

export function App() {
  const route = useHashRoute()

  // Scroll handling on navigation: doc routes start at the top; the home route honors a plain
  // "#section" fragment (so cross-page nav like Diagrams/Cookbook still scrolls once home mounts).
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
      <Nav />

      {route.name === 'home' && <Home />}
      {route.name === 'docs-index' && <DocsIndex />}
      {route.name === 'doc' && route.slug ? <DocPage slug={route.slug} /> : null}

      <footer className="site-footer">
        <p className="attribution">
          Built with care by <a href={photonGroveUrl}>Photon Grove</a> — a Colorado software studio.
        </p>
      </footer>
    </main>
  )
}
