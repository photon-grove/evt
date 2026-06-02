import {DiagramViewer} from '@photon-grove/react-flow-diagrams'

import {EventGarden, ToolkitShelf} from './ClipArt'
import {capabilities, cookbook, examples, gettingStarted} from './content'
import {diagrams} from './diagrams'

export function App() {
  return (
    <main>
      <header className="nav">
        <a className="brand" href="#top" aria-label="evt home">
          <span className="brand-mark">evt</span>
          <span>Event sourcing for Go</span>
        </a>
        <nav aria-label="Primary navigation">
          <a href="#docs">Docs</a>
          <a href="#diagrams">Diagrams</a>
          <a href="#cookbook">Cookbook</a>
          <a href="https://github.com/photon-grove/evt">GitHub</a>
        </nav>
      </header>

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
            <a className="button primary" href="#docs">Start building</a>
            <a className="button secondary" href="#diagrams">Explore architecture</a>
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
        </div>
        <div className="capability-grid">
          {capabilities.map((item) => (
            <article className="feature-card" key={item.title}>
              <h3>{item.title}</h3>
              <p>{item.body}</p>
            </article>
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
    </main>
  )
}
