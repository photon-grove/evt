import {useCallback, useEffect, useState} from 'react'

import type {Lang} from './content'

export type Theme = 'light' | 'dark'

const THEME_KEY = 'evt-theme'
const LANG_KEY = 'evt-lang'

// Storage can be missing or throw (private windows, blocked site data), so every access is
// guarded and the site renders the same without it.
function read(key: string): string | null {
  try {
    return window.localStorage.getItem(key)
  } catch {
    return null
  }
}

function write(key: string, value: string) {
  try {
    window.localStorage.setItem(key, value)
  } catch {
    // Not persisted; the choice still applies for this visit.
  }
}

function systemTheme(): Theme {
  return window.matchMedia?.('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

function storedTheme(): Theme | null {
  const value = read(THEME_KEY)

  return value === 'light' || value === 'dark' ? value : null
}

// useTheme follows the system theme until the reader picks one, then remembers the pick. The
// choice lives on <html data-theme>, which index.html also sets before first paint.
export function useTheme(): [Theme, () => void] {
  const [theme, setTheme] = useState<Theme>(() => storedTheme() ?? systemTheme())

  useEffect(() => {
    const media = window.matchMedia?.('(prefers-color-scheme: dark)')
    if (!media) {
      return
    }

    const onChange = () => {
      if (!storedTheme()) {
        setTheme(systemTheme())
      }
    }

    media.addEventListener('change', onChange)

    return () => media.removeEventListener('change', onChange)
  }, [])

  const toggle = useCallback(() => {
    setTheme((current) => {
      const next = current === 'dark' ? 'light' : 'dark'
      write(THEME_KEY, next)
      document.documentElement.dataset.theme = next

      return next
    })
  }, [])

  return [theme, toggle]
}

// useLang is the reader's language for every code sample on the landing page.
export function useLang(): [Lang, (lang: Lang) => void] {
  const [lang, setLang] = useState<Lang>(() => (read(LANG_KEY) === 'rust' ? 'rust' : 'go'))

  const choose = useCallback((next: Lang) => {
    write(LANG_KEY, next)
    setLang(next)
  }, [])

  return [lang, choose]
}
