import type {FeatureIcon} from './content'

// EvtMark is the logo: three appended log rows, the newest in teal, and a gold head marker. Its
// colors come from CSS tokens, so it inverts with the theme.
export function EvtMark({size = 32}: {size?: number}) {
  return (
    <svg className="evt-mark" width={size} height={size} viewBox="0 0 64 64" aria-hidden="true">
      <rect width="64" height="64" rx="15" className="evt-mark-bg" />
      <rect x="13" y="15" width="22" height="8" rx="4" className="evt-mark-row" opacity="0.55" />
      <rect x="13" y="28" width="30" height="8" rx="4" className="evt-mark-row" opacity="0.8" />
      <rect x="13" y="41" width="38" height="8" rx="4" className="evt-mark-new" />
      <circle cx="51" cy="19" r="5" className="evt-mark-head" />
    </svg>
  )
}

// Hero diagram: an append-only event log (snapshot at sk 0 plus immutable event rows) projected
// into a rebuildable read model. The last cell is appended on a loop, and the view row it feeds
// pulses, unless the reader prefers reduced motion.
export function EventLogArt() {
  const cells = [
    {x: 28, label: 'sk 0', kind: 'snapshot'},
    {x: 112, label: 'sk 1', kind: 'event'},
    {x: 196, label: 'sk 2', kind: 'event'},
    {x: 280, label: 'sk 3', kind: 'event'},
    {x: 364, label: 'sk 4', kind: 'event', fresh: true},
  ]

  return (
    <svg
      className="clip clip-log"
      viewBox="0 0 460 380"
      role="img"
      aria-label="An append-only event log with a snapshot at sk 0, projected into a rebuildable view"
    >
      <rect x="4" y="4" width="452" height="372" rx="20" className="art-panel" />

      <text x="28" y="44" className="art-label art-label-strong">
        event-log
      </text>
      <text x="432" y="44" textAnchor="end" className="art-meta">
        append-only
      </text>

      {cells.map((cell, index) => {
        const isSnapshot = cell.kind === 'snapshot'

        return (
          <g key={cell.label} className={cell.fresh ? 'art-fresh' : undefined}>
            {index > 0 ? <path d={`M${cell.x - 12} 87h8`} className="art-link" /> : null}
            <rect
              x={cell.x}
              y={60}
              width={68}
              height={54}
              rx={10}
              className={isSnapshot ? 'art-cell-box art-cell-snapshot' : 'art-cell-box'}
            />
            <text x={cell.x + 34} y={84} textAnchor="middle" className={isSnapshot ? 'art-cell art-on-accent' : 'art-cell'}>
              {cell.label}
            </text>
            <text x={cell.x + 34} y={101} textAnchor="middle" className={isSnapshot ? 'art-cell-sub art-on-accent' : 'art-cell-sub'}>
              {isSnapshot ? 'snapshot' : 'event'}
            </text>
          </g>
        )
      })}

      <path d="M230 128v44" className="art-flow" />
      <path d="M224 168l6 8 6-8" className="art-arrow" />
      <text x="244" y="156" className="art-meta">
        project · replay
      </text>

      <rect x="76" y="194" width="308" height="150" rx="14" className="art-card" />
      <text x="100" y="226" className="art-label art-label-strong">
        views
      </text>
      <text x="360" y="226" textAnchor="end" className="art-meta art-meta-accent">
        rebuildable
      </text>

      {[0, 1, 2, 3].map((row) => (
        <g key={row} className={row === 0 ? 'art-row-live' : undefined}>
          <rect x="100" y={244 + row * 22} width="14" height="14" rx="4" className={row === 0 ? 'art-dot art-dot-live' : 'art-dot'} />
          <rect x="126" y={248 + row * 22} width={[196, 150, 172, 120][row]} height="6" rx="3" className="art-bar" />
          <rect x={[332, 286, 308, 256][row]} y={248 + row * 22} width="26" height="6" rx="3" className="art-bar-accent" />
        </g>
      ))}
    </svg>
  )
}

// FeatureGlyph draws the small line icon on each capability card.
export function FeatureGlyph({icon}: {icon: FeatureIcon}) {
  return (
    <svg className="feature-glyph" viewBox="0 0 24 24" aria-hidden="true">
      {GLYPHS[icon]}
    </svg>
  )
}

const GLYPHS: Record<FeatureIcon, React.ReactNode> = {
  commit: (
    <>
      <circle cx="12" cy="12" r="3.5" />
      <path d="M3 12h5.5M15.5 12H21" />
    </>
  ),
  streams: (
    <>
      <path d="M4 6h10M4 12h13M4 18h8" />
      <path d="M19 4v16" />
    </>
  ),
  outcome: (
    <>
      <rect x="4" y="4" width="16" height="16" rx="3" />
      <path d="M8.5 12.5l2.5 2.5 5-6" />
    </>
  ),
  view: (
    <>
      <rect x="3.5" y="5" width="17" height="14" rx="2.5" />
      <path d="M3.5 10h17M9 10v9" />
    </>
  ),
  snapshot: (
    <>
      <path d="M4 8V5.5A1.5 1.5 0 0 1 5.5 4H8M16 4h2.5A1.5 1.5 0 0 1 20 5.5V8M20 16v2.5a1.5 1.5 0 0 1-1.5 1.5H16M8 20H5.5A1.5 1.5 0 0 1 4 18.5V16" />
      <circle cx="12" cy="12" r="3" />
    </>
  ),
  fence: (
    <>
      <path d="M6 20V7l2-3 2 3v13M14 20V7l2-3 2 3v13" />
      <path d="M4 10h16M4 16h16" />
    </>
  ),
  journal: (
    <>
      <path d="M6 4h10a2 2 0 0 1 2 2v14H8a2 2 0 0 1-2-2z" />
      <path d="M10 9h5M10 13h5" />
    </>
  ),
  lambda: (
    <>
      <path d="M7 4h3l7 16" />
      <path d="M12.5 10.5L7 20" />
    </>
  ),
  compat: (
    <>
      <path d="M7 7h11l-3-3M17 17H6l3 3" />
    </>
  ),
}
