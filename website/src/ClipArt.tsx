// Hero illustration for the docs site.
//
// It draws the core mental model of evt: an append-only event log (a durable
// snapshot at sk=0 followed by immutable event rows) that projects into a
// rebuildable read model. Kept deliberately schematic so it reads as a systems
// diagram, not decoration.
export function EventLogArt() {
  const cells = [
    {x: 40, label: 'sk 0', kind: 'snapshot'},
    {x: 124, label: 'sk 1', kind: 'event'},
    {x: 208, label: 'sk 2', kind: 'event'},
    {x: 292, label: 'sk 3', kind: 'event'},
    {x: 376, label: 'sk 4', kind: 'event'},
  ]

  return (
    <svg
      className="clip clip-log"
      viewBox="0 0 500 380"
      role="img"
      aria-label="An append-only event log with a snapshot at sk 0, projected into a rebuildable view"
    >
      <defs>
        <linearGradient id="log-panel" x1="0" x2="1" y1="0" y2="1">
          <stop offset="0" stopColor="#fffdf8" />
          <stop offset="1" stopColor="#f1f7f4" />
        </linearGradient>
      </defs>

      <rect x="4" y="4" width="492" height="372" rx="20" fill="url(#log-panel)" stroke="rgba(23,32,51,0.14)" />

      <text x="40" y="48" className="art-label art-label-strong">event-log</text>
      <text x="416" y="48" className="art-meta">append-only</text>

      {cells.map((cell, index) => {
        const isSnapshot = cell.kind === 'snapshot'
        return (
          <g key={cell.label}>
            {index > 0 ? (
              <path
                d={`M${cell.x - 10} 92h8`}
                stroke="#0f766e"
                strokeWidth="2.5"
                strokeLinecap="round"
                opacity="0.5"
              />
            ) : null}
            <rect
              x={cell.x}
              y={64}
              width={68}
              height={56}
              rx={10}
              fill={isSnapshot ? '#0f766e' : '#ffffff'}
              stroke={isSnapshot ? '#0f766e' : 'rgba(23,32,51,0.55)'}
              strokeWidth="2"
            />
            <text
              x={cell.x + 34}
              y={88}
              textAnchor="middle"
              className="art-cell"
              style={{fill: isSnapshot ? '#fffdf8' : '#172033'}}
            >
              {cell.label}
            </text>
            <text
              x={cell.x + 34}
              y={106}
              textAnchor="middle"
              className="art-cell-sub"
              style={{fill: isSnapshot ? 'rgba(255,253,248,0.78)' : 'rgba(23,32,51,0.5)'}}
            >
              {isSnapshot ? 'snapshot' : 'event'}
            </text>
          </g>
        )
      })}

      {/* projection flow */}
      <path
        d="M250 132v40q0 12 12 12h0"
        fill="none"
        stroke="#0f766e"
        strokeWidth="2.5"
        strokeLinecap="round"
        strokeDasharray="3 6"
      />
      <path d="M250 132v54" fill="none" stroke="#0f766e" strokeWidth="2.5" strokeLinecap="round" strokeDasharray="3 6" />
      <path d="M244 182l6 8 6-8" fill="none" stroke="#0f766e" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
      <text x="262" y="170" className="art-meta">replay · project</text>

      <rect x="96" y="206" width="308" height="138" rx="14" fill="#ffffff" stroke="rgba(23,32,51,0.14)" />
      <text x="120" y="238" className="art-label art-label-strong">entity-views</text>
      <text x="380" y="238" textAnchor="end" className="art-meta art-meta-accent">rebuildable</text>

      {[0, 1, 2].map((row) => (
        <g key={row}>
          <rect x="120" y={258 + row * 26} width="14" height="14" rx="4" fill={row === 0 ? '#0f766e' : '#cfe6df'} />
          <rect x="146" y={262 + row * 26} width={row === 1 ? 150 : 196} height="6" rx="3" fill="rgba(23,32,51,0.22)" />
          <rect x={row === 1 ? 306 : 352} y={262 + row * 26} width={row === 1 ? 30 : 26} height="6" rx="3" fill="#cfe6df" />
        </g>
      ))}
    </svg>
  )
}
