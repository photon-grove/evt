import {describe, expect, it} from 'vitest'

import {
  type ColorSchemeQuery,
  followSystemTheme,
  initialLang,
  initialTheme,
  LANG_KEY,
  type PreferenceStore,
  THEME_KEY,
  toggledTheme,
  writePreference,
} from './preferences'

function memoryStore(entries: Record<string, string> = {}): PreferenceStore & {entries: Record<string, string>} {
  return {
    entries,
    getItem: (key) => entries[key] ?? null,
    setItem: (key, value) => {
      entries[key] = value
    },
  }
}

// blockedStore throws on every access, like storage in a private window with site data blocked.
const blockedStore: PreferenceStore = {
  getItem: () => {
    throw new Error('blocked')
  },
  setItem: () => {
    throw new Error('blocked')
  },
}

function fakeQuery(matches: boolean): ColorSchemeQuery & {listeners: Set<() => void>; flip: (dark: boolean) => void} {
  const listeners = new Set<() => void>()
  const query = {
    matches,
    listeners,
    addEventListener: (_: string, listener: () => void) => listeners.add(listener),
    removeEventListener: (_: string, listener: () => void) => listeners.delete(listener),
    flip: (dark: boolean) => {
      query.matches = dark
      listeners.forEach((listener) => listener())
    },
  }

  return query as unknown as ColorSchemeQuery & {listeners: Set<() => void>; flip: (dark: boolean) => void}
}

describe('initialTheme', () => {
  it('follows the system when the reader has not picked', () => {
    expect(initialTheme(memoryStore(), fakeQuery(true))).toBe('dark')
    expect(initialTheme(memoryStore(), fakeQuery(false))).toBe('light')
    expect(initialTheme(memoryStore(), null)).toBe('light')
  })

  it('prefers a saved pick and ignores an unknown value', () => {
    expect(initialTheme(memoryStore({[THEME_KEY]: 'light'}), fakeQuery(true))).toBe('light')
    expect(initialTheme(memoryStore({[THEME_KEY]: 'sepia'}), fakeQuery(true))).toBe('dark')
  })

  it('falls back to the system when storage is blocked or missing', () => {
    expect(initialTheme(blockedStore, fakeQuery(true))).toBe('dark')
    expect(initialTheme(null, fakeQuery(false))).toBe('light')
  })
})

describe('toggledTheme and writePreference', () => {
  it('flips the theme and remembers the pick', () => {
    const store = memoryStore()
    const next = toggledTheme('light')
    writePreference(store, THEME_KEY, next)

    expect(next).toBe('dark')
    expect(toggledTheme(next)).toBe('light')
    expect(initialTheme(store, fakeQuery(false))).toBe('dark')
  })

  it('does not throw when storage is blocked', () => {
    expect(() => writePreference(blockedStore, THEME_KEY, 'dark')).not.toThrow()
    expect(() => writePreference(null, THEME_KEY, 'dark')).not.toThrow()
  })
})

describe('followSystemTheme', () => {
  it('reports system changes until the reader picks a theme', () => {
    const store = memoryStore()
    const query = fakeQuery(false)
    const seen: string[] = []
    followSystemTheme(store, query, (theme) => seen.push(theme))

    query.flip(true)
    store.entries[THEME_KEY] = 'light'
    query.flip(false)

    expect(seen).toEqual(['dark'])
  })

  it('stops listening once unsubscribed', () => {
    const query = fakeQuery(false)
    const seen: string[] = []
    const unsubscribe = followSystemTheme(memoryStore(), query, (theme) => seen.push(theme))

    unsubscribe()
    query.flip(true)

    expect(query.listeners.size).toBe(0)
    expect(seen).toEqual([])
  })

  it('keeps following the system when storage is blocked', () => {
    const query = fakeQuery(false)
    const seen: string[] = []
    followSystemTheme(blockedStore, query, (theme) => seen.push(theme))

    query.flip(true)

    expect(seen).toEqual(['dark'])
  })

  it('is a no-op without matchMedia', () => {
    expect(() => followSystemTheme(memoryStore(), null, () => {})()).not.toThrow()
  })
})

describe('initialLang', () => {
  it('defaults to Rust and keeps a saved Go pick', () => {
    expect(initialLang(memoryStore())).toBe('rust')
    expect(initialLang(memoryStore({[LANG_KEY]: 'go'}))).toBe('go')
    expect(initialLang(memoryStore({[LANG_KEY]: 'python'}))).toBe('rust')
    expect(initialLang(blockedStore)).toBe('rust')
  })
})
