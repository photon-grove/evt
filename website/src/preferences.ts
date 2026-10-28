import {useCallback, useEffect, useState} from 'react'

import type {Lang} from './content'

export type Theme = 'light' | 'dark'

export const THEME_KEY = 'evt-theme'
export const LANG_KEY = 'evt-lang'

// PreferenceStore is the part of Storage the preferences use, so tests can pass a fake.
export type PreferenceStore = Pick<Storage, 'getItem' | 'setItem'>

// ColorSchemeQuery is the part of MediaQueryList the theme follows.
export type ColorSchemeQuery = Pick<MediaQueryList, 'matches' | 'addEventListener' | 'removeEventListener'>

// Storage can be missing or throw (private windows, blocked site data), so every access is
// guarded and the site renders the same without it.
export function readPreference(store: PreferenceStore | null, key: string): string | null {
  try {
    return store?.getItem(key) ?? null
  } catch {
    return null
  }
}

export function writePreference(store: PreferenceStore | null, key: string, value: string) {
  try {
    store?.setItem(key, value)
  } catch {
    // Not persisted; the choice still applies for this visit.
  }
}

export function storedTheme(store: PreferenceStore | null): Theme | null {
  const value = readPreference(store, THEME_KEY)

  return value === 'light' || value === 'dark' ? value : null
}

// initialTheme is the saved pick, or the system theme when the reader hasn't picked one.
export function initialTheme(store: PreferenceStore | null, query: ColorSchemeQuery | null): Theme {
  return storedTheme(store) ?? (query?.matches ? 'dark' : 'light')
}

export function toggledTheme(theme: Theme): Theme {
  return theme === 'dark' ? 'light' : 'dark'
}

// followSystemTheme reports system theme changes until the reader picks a theme, and returns the
// unsubscribe.
export function followSystemTheme(
  store: PreferenceStore | null,
  query: ColorSchemeQuery | null,
  onChange: (theme: Theme) => void,
): () => void {
  if (!query) {
    return () => {}
  }

  const listener = () => {
    if (!storedTheme(store)) {
      onChange(query.matches ? 'dark' : 'light')
    }
  }

  query.addEventListener('change', listener)

  return () => query.removeEventListener('change', listener)
}

// initialLang is Rust unless the reader picked Go.
export function initialLang(store: PreferenceStore | null): Lang {
  return readPreference(store, LANG_KEY) === 'go' ? 'go' : 'rust'
}

// Reading window.localStorage itself throws when site data is blocked.
function localStore(): PreferenceStore | null {
  try {
    return window.localStorage
  } catch {
    return null
  }
}

function colorSchemeQuery(): ColorSchemeQuery | null {
  return window.matchMedia?.('(prefers-color-scheme: dark)') ?? null
}

// useTheme follows the system theme until the reader picks one, then remembers the pick. The
// choice lives on <html data-theme>, which index.html also sets before first paint.
export function useTheme(): [Theme, () => void] {
  const [theme, setTheme] = useState<Theme>(() => initialTheme(localStore(), colorSchemeQuery()))

  useEffect(() => followSystemTheme(localStore(), colorSchemeQuery(), setTheme), [])

  const toggle = useCallback(() => {
    setTheme((current) => {
      const next = toggledTheme(current)
      writePreference(localStore(), THEME_KEY, next)
      document.documentElement.dataset.theme = next

      return next
    })
  }, [])

  return [theme, toggle]
}

// useLang is the reader's language for every code sample on the landing page.
export function useLang(): [Lang, (lang: Lang) => void] {
  const [lang, setLang] = useState<Lang>(() => initialLang(localStore()))

  const choose = useCallback((next: Lang) => {
    writePreference(localStore(), LANG_KEY, next)
    setLang(next)
  }, [])

  return [lang, choose]
}
