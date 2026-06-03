export function EventGarden() {
  return (
    <svg className="clip clip-garden" viewBox="0 0 520 420" role="img" aria-label="Event streams growing into read models">
      <defs>
        <linearGradient id="garden-sky" x1="0" x2="1" y1="0" y2="1">
          <stop offset="0" stopColor="#fef3c7" />
          <stop offset="0.54" stopColor="#dbeafe" />
          <stop offset="1" stopColor="#dcfce7" />
        </linearGradient>
        <linearGradient id="garden-leaf" x1="0" x2="1">
          <stop offset="0" stopColor="#0f766e" />
          <stop offset="1" stopColor="#65a30d" />
        </linearGradient>
      </defs>
      <rect x="18" y="18" width="484" height="384" rx="28" fill="url(#garden-sky)" />
      <path d="M66 306c68-24 130-30 186-18 58 12 123 7 198-25v72H66z" fill="#164e63" opacity="0.16" />
      <path d="M80 308c80-48 157-62 232-42 44 12 83 8 126-15" fill="none" stroke="#0f766e" strokeWidth="8" strokeLinecap="round" />
      <g fill="#ffffff" stroke="#0f172a" strokeWidth="4">
        <rect x="78" y="96" width="110" height="74" rx="18" />
        <rect x="220" y="70" width="110" height="74" rx="18" />
        <rect x="348" y="116" width="94" height="70" rx="18" />
      </g>
      <g fill="#fed7aa" stroke="#0f172a" strokeWidth="4">
        <circle cx="112" cy="224" r="18" />
        <circle cx="188" cy="228" r="18" />
        <circle cx="264" cy="215" r="18" />
        <circle cx="342" cy="223" r="18" />
      </g>
      <g fill="none" stroke="#0f172a" strokeLinecap="round" strokeLinejoin="round" strokeWidth="5">
        <path d="M104 224c6 26 16 48 34 67" />
        <path d="M190 228c14 24 31 41 52 52" />
        <path d="M268 215c18 26 38 43 60 52" />
        <path d="M344 223c18 18 30 37 38 58" />
      </g>
      <g fill="url(#garden-leaf)">
        <path d="M132 272c-26-24-48-28-67-11 24 24 46 27 67 11z" />
        <path d="M238 276c-29-14-51-12-67 6 28 15 51 13 67-6z" />
        <path d="M326 264c-19-24-39-31-61-20 19 24 40 30 61 20z" />
        <path d="M386 278c-4-30-19-48-45-55 3 31 18 49 45 55z" />
      </g>
      <g fill="#0f172a">
        <circle cx="116" cy="128" r="5" />
        <circle cx="138" cy="128" r="5" />
        <circle cx="160" cy="128" r="5" />
        <path d="M250 100h50v10h-50zM250 120h34v10h-34z" />
        <path d="M378 146h34v8h-34zM378 164h20v8h-20z" />
      </g>
    </svg>
  )
}

export function ToolkitShelf() {
  return (
    <svg className="clip clip-shelf" viewBox="0 0 520 220" role="img" aria-label="Reusable evt package toolkit">
      <rect x="24" y="32" width="472" height="156" rx="24" fill="#fff7ed" />
      <path d="M54 160h412" stroke="#0f172a" strokeWidth="6" strokeLinecap="round" />
      {[
        ['#bfdbfe', 78, 72, 70, 88],
        ['#bbf7d0', 158, 58, 72, 102],
        ['#fde68a', 240, 82, 68, 78],
        ['#fecdd3', 318, 64, 74, 96],
        ['#ddd6fe', 402, 76, 54, 84],
      ].map(([fill, x, y, w, h], index) => (
        <g key={index}>
          <rect x={x as number} y={y as number} width={w as number} height={h as number} rx="10" fill={fill as string} stroke="#0f172a" strokeWidth="5" />
          <path d={`M${(x as number) + 18} ${(y as number) + 24}h${(w as number) - 36}`} stroke="#0f172a" strokeWidth="4" strokeLinecap="round" />
          <path d={`M${(x as number) + 18} ${(y as number) + 44}h${(w as number) - 28}`} stroke="#0f172a" strokeWidth="4" strokeLinecap="round" opacity="0.55" />
        </g>
      ))}
      <circle cx="61" cy="63" r="14" fill="#fb923c" stroke="#0f172a" strokeWidth="5" />
      <path d="M461 57l18 18-18 18-18-18z" fill="#14b8a6" stroke="#0f172a" strokeWidth="5" />
    </svg>
  )
}
