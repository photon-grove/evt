import {describe, expect, it} from 'vitest'

import {humanizeSlug, resolveDocLink, slugifyHeading, titleFromMarkdown} from './markdown'
import {parseHash} from './useHashRoute'
import {docBySlug, docs, slugFromPath} from './docs'

describe('slugifyHeading', () => {
  it('matches the in-page anchors the docs already use', () => {
    expect(slugifyHeading('Constant-memory enumeration')).toBe('constant-memory-enumeration')
    expect(slugifyHeading('Rebuilding compacted streams')).toBe('rebuilding-compacted-streams')
    expect(slugifyHeading('StreamEntitiesFromSnapshots (notes)')).toBe('streamentitiesfromsnapshots-notes')
  })
})

describe('titleFromMarkdown', () => {
  it('returns the first H1', () => {
    expect(titleFromMarkdown('# Projections and Rebuilds\n\nbody')).toBe('Projections and Rebuilds')
  })

  it('ignores deeper headings and returns undefined when there is no H1', () => {
    expect(titleFromMarkdown('## Subsection\n\nbody')).toBeUndefined()
  })
})

describe('humanizeSlug', () => {
  it('title-cases the final path segment', () => {
    expect(humanizeSlug('getting-started')).toBe('Getting Started')
    expect(humanizeSlug('adr/0001-event-compaction')).toBe('0001 Event Compaction')
  })
})

describe('slugFromPath', () => {
  it('strips the docs prefix and .md extension, preserving nesting', () => {
    expect(slugFromPath('../../docs/guide/projections.md')).toBe('guide/projections')
    expect(slugFromPath('../../docs/adr/0001-event-compaction-and-snapshot-truncation.md')).toBe(
      'adr/0001-event-compaction-and-snapshot-truncation',
    )
  })
})

describe('resolveDocLink', () => {
  it('treats protocol and protocol-relative URLs as external', () => {
    expect(resolveDocLink('https://example.com', 'projections')).toEqual({
      kind: 'external',
      href: 'https://example.com',
    })
    expect(resolveDocLink('//cdn.example.com/x', 'projections').kind).toBe('external')
  })

  it('treats pure fragments as in-page anchors', () => {
    expect(resolveDocLink('#constant-memory-enumeration', 'projections')).toEqual({
      kind: 'anchor',
      id: 'constant-memory-enumeration',
    })
  })

  it('routes a sibling .md link relative to the current doc directory', () => {
    expect(resolveDocLink('adr/0001-event-compaction.md', 'projections')).toEqual({
      kind: 'route',
      slug: 'adr/0001-event-compaction',
    })
  })

  it('resolves ../ against a nested doc', () => {
    expect(resolveDocLink('../concepts.md', 'adr/0001-x')).toEqual({
      kind: 'route',
      slug: 'concepts',
    })
  })

  it('drops a trailing fragment on a .md route link', () => {
    expect(resolveDocLink('streams.md#projectors', 'projections')).toEqual({
      kind: 'route',
      slug: 'streams',
    })
  })

  it('leaves non-markdown relative links untouched', () => {
    expect(resolveDocLink('../infra/local/main.tf', 'projections')).toEqual({
      kind: 'asis',
      href: '../infra/local/main.tf',
    })
  })
})

describe('parseHash', () => {
  it('maps non-route hashes (and scroll anchors) to home', () => {
    expect(parseHash('')).toEqual({name: 'home'})
    expect(parseHash('#diagrams')).toEqual({name: 'home'})
    expect(parseHash('#/')).toEqual({name: 'home'})
  })

  it('parses the docs index and individual docs, preserving nested slugs', () => {
    expect(parseHash('#/docs')).toEqual({name: 'docs-index'})
    expect(parseHash('#/docs/projections')).toEqual({name: 'doc', slug: 'projections'})
    expect(parseHash('#/docs/adr/0001-x')).toEqual({name: 'doc', slug: 'adr/0001-x'})
  })
})

describe('published documents', () => {
  it('include the guides, the reference pages and the ADRs', () => {
    expect(docBySlug('guide/getting-started')).toBeDefined()
    expect(docBySlug('reference/storage-format')).toBeDefined()
    expect(docs.some((doc) => doc.slug.startsWith('adr/'))).toBe(true)
  })

  it('leave out docs/internal, which holds agent and process notes', () => {
    expect(docs.filter((doc) => doc.slug.startsWith('internal/'))).toEqual([])
    expect(docBySlug('internal/reorg-plan')).toBeUndefined()
  })
})
