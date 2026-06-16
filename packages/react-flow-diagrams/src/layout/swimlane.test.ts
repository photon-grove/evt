import {describe, expect, it} from 'vitest'

import type {DiagramSpec} from '../types'
import {runSwimlaneLayout} from './swimlane'

const spec: DiagramSpec = {
  id: 'test',
  title: 'Test',
  layout: {
    lanes: [
      {id: 'top', label: 'Top'},
      {id: 'mid', label: 'Middle'},
      {id: 'bottom', label: 'Bottom'},
    ],
  },
  nodes: [
    {id: 'a', kind: 'client', label: 'A', lane: 'top'},
    {id: 'b', kind: 'service', label: 'B', lane: 'mid'},
    {id: 'c', kind: 'process', label: 'C', lane: 'mid'},
    {id: 'd', kind: 'datastore', label: 'D', lane: 'bottom'},
  ],
  edges: [
    {id: 'a-b', source: 'a', target: 'b', variant: 'request'},
    {id: 'b-c', source: 'b', target: 'c', variant: 'data'},
    {id: 'c-d', source: 'c', target: 'd', variant: 'data'},
  ],
}

describe('runSwimlaneLayout', () => {
  it('emits one lane band per lane, ahead of the cards', () => {
    const {nodes} = runSwimlaneLayout(spec)

    const lanes = nodes.filter((n) => n.type === 'lane')
    expect(lanes.map((n) => n.id)).toEqual(['lane:top', 'lane:mid', 'lane:bottom'])

    // React Flow requires a parent to appear before its children.
    const lastLane = nodes.findLastIndex((n) => n.type === 'lane')
    const firstCard = nodes.findIndex((n) => n.type !== 'lane')
    expect(lastLane).toBeLessThan(firstCard)
  })

  it('parents each card to its lane band and pins it inside', () => {
    const {nodes} = runSwimlaneLayout(spec)

    const a = nodes.find((n) => n.id === 'a')
    expect(a?.parentId).toBe('lane:top')
    expect(a?.extent).toBe('parent')

    const c = nodes.find((n) => n.id === 'c')
    expect(c?.parentId).toBe('lane:mid')
  })

  it('orders columns by longest-path depth so the flow reads left-to-right', () => {
    const {nodes} = runSwimlaneLayout(spec)
    const byId = new Map(nodes.map((n) => [n.id, n]))

    // Cards are parented to lane bands, but every band sits at x=0, so a card's
    // parent-relative position.x equals its absolute x here. The chain a→b→c→d
    // must be strictly increasing in x.
    const absX = (id: string): number => byId.get(id)!.position.x
    expect(absX('a')).toBeLessThan(absX('b'))
    expect(absX('b')).toBeLessThan(absX('c'))
    expect(absX('c')).toBeLessThan(absX('d'))
  })

  it('stacks lane bands vertically without overlap', () => {
    const {nodes} = runSwimlaneLayout(spec)
    const lanes = nodes.filter((n) => n.type === 'lane')

    const height = lanes[0]?.height ?? 0
    expect(height).toBeGreaterThan(0)
    expect(lanes[0]?.position.y).toBe(0)
    expect(lanes[1]?.position.y).toBe(height)
    expect(lanes[2]?.position.y).toBe(height * 2)
    // Every band is the same width (full grid width).
    const widths = new Set(lanes.map((n) => n.width))
    expect(widths.size).toBe(1)
  })

  it('stacks cards that share a lane and column instead of overlapping', () => {
    // `x` and `y` both sit in the top lane at column 0 (no edges → depth 0).
    const shared: DiagramSpec = {
      id: 'shared',
      title: 'Shared cell',
      layout: {lanes: [{id: 'top', label: 'Top'}, {id: 'bottom', label: 'Bottom'}]},
      nodes: [
        {id: 'x', kind: 'service', label: 'X', lane: 'top'},
        {id: 'y', kind: 'service', label: 'Y', lane: 'top'},
        {id: 'z', kind: 'datastore', label: 'Z', lane: 'bottom'},
      ],
      edges: [],
    }

    const {nodes} = runSwimlaneLayout(shared)
    const x = nodes.find((n) => n.id === 'x')!
    const y = nodes.find((n) => n.id === 'y')!
    const lanes = nodes.filter((n) => n.type === 'lane')

    // Same column, different vertical slot — the two cards do not overlap.
    expect(x.position.x).toBe(y.position.x)
    const gap = Math.abs((y.position.y ?? 0) - (x.position.y ?? 0))
    expect(gap).toBeGreaterThanOrEqual(x.height ?? 0)

    // The crowded top lane is taller than the single-card bottom lane.
    const top = lanes.find((n) => n.id === 'lane:top')!
    const bottom = lanes.find((n) => n.id === 'lane:bottom')!
    expect(top.height ?? 0).toBeGreaterThan(bottom.height ?? 0)
    // Bands still stack without overlap.
    expect(bottom.position.y).toBe(top.height)
  })

  it('keeps edge styling and routes them as smoothstep', () => {
    const {edges} = runSwimlaneLayout(spec)

    expect(edges).toHaveLength(3)
    for (const edge of edges) {
      expect(edge.type).toBe('smoothstep')
      expect(edge.sourceHandle).toBe('out')
      expect(edge.targetHandle).toBe('in')
    }
    expect(edges.find((e) => e.id === 'a-b')?.markerEnd).toBeDefined()
  })
})
