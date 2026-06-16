import {useEffect, useState} from 'react'

// Route is the in-site location. Hash-based so it works under GitHub Pages' /evt/ base with no
// server rewrites, and so it never collides with the landing page's plain "#section" scroll anchors
// (those don't start with "/", so they resolve to the home route and scroll natively).
export interface Route {
  name: 'home' | 'docs-index' | 'doc'
  slug?: string
}

export function parseHash(hash: string): Route {
  const path = hash.replace(/^#/, '')
  if (!path.startsWith('/')) {
    return {name: 'home'}
  }

  const parts = path.split('/').filter(Boolean)
  if (parts[0] !== 'docs') {
    return {name: 'home'}
  }

  if (parts.length === 1) {
    return {name: 'docs-index'}
  }

  return {name: 'doc', slug: parts.slice(1).join('/')}
}

export function useHashRoute(): Route {
  const [route, setRoute] = useState<Route>(() =>
    parseHash(typeof window === 'undefined' ? '' : window.location.hash),
  )

  useEffect(() => {
    const onHashChange = () => setRoute(parseHash(window.location.hash))

    window.addEventListener('hashchange', onHashChange)

    return () => window.removeEventListener('hashchange', onHashChange)
  }, [])

  return route
}
