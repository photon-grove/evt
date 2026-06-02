import {describe, expect, it} from 'vitest'

import type {DiagramSpec} from '../types'
import {runElkLayout} from './elk'

const spec: DiagramSpec = {
  id: 'test',
  title: 'Test',
  nodes: [
    {id: 'box', kind: 'boundary', label: 'Group', domain: 'web'},
    {id: 'a', kind: 'client', label: 'Browser', domain: 'web', parent: 'box'},
    // `d` lives in the same boundary as `a`, so the a→d edge is boundary-internal
    // and exercises the common-ancestor coordinate offset.
    {id: 'd', kind: 'service', label: 'Edge', domain: 'web', parent: 'box'},
    {id: 'b', kind: 'service', label: 'API', domain: 'api'},
    {id: 'c', kind: 'datastore', label: 'DB', domain: 'data'},
  ],
  edges: [
    {id: 'a-d', source: 'a', target: 'd', variant: 'data', label: 'local'},
    {id: 'a-b', source: 'a', target: 'b', variant: 'request'},
    {id: 'b-c', source: 'b', target: 'c', variant: 'data', label: 'query'},
  ],
}

describe('runElkLayout', () => {
  it('assigns a position to every node and types them by kind', async () => {
    const {nodes} = await runElkLayout(spec)

    expect(nodes).toHaveLength(5)
    for (const node of nodes) {
      expect(Number.isFinite(node.position.x)).toBe(true)
      expect(Number.isFinite(node.position.y)).toBe(true)
    }
    expect(nodes.find((n) => n.id === 'b')?.type).toBe('service')
  })

  it('nests child nodes under their boundary parent', async () => {
    const {nodes} = await runElkLayout(spec)

    const child = nodes.find((n) => n.id === 'a')
    expect(child?.parentId).toBe('box')
    expect(child?.extent).toBe('parent')

    // Parent must appear before its child in the array (React Flow requirement).
    const parentIndex = nodes.findIndex((n) => n.id === 'box')
    const childIndex = nodes.findIndex((n) => n.id === 'a')
    expect(parentIndex).toBeLessThan(childIndex)
  })

  it('styles edges by variant and adds handles', async () => {
    const {edges} = await runElkLayout(spec)

    expect(edges).toHaveLength(3)
    const request = edges.find((e) => e.id === 'a-b')
    expect(request?.sourceHandle).toBe('out')
    expect(request?.targetHandle).toBe('in')
    expect(request?.markerEnd).toBeDefined()
  })

  it('routes edges along ELK waypoints in flow coordinates', async () => {
    const {edges} = await runElkLayout(spec)

    for (const edge of edges) {
      expect(edge.type).toBe('orthogonal')
      const points = (edge.data as {points?: {x: number; y: number}[]} | undefined)?.points
      expect(points?.length ?? 0).toBeGreaterThanOrEqual(2)
      for (const point of points ?? []) {
        expect(Number.isFinite(point.x)).toBe(true)
        expect(Number.isFinite(point.y)).toBe(true)
      }
    }
  })

  it('gives a labeled edge a finite label position', async () => {
    const {edges} = await runElkLayout(spec)

    const labeled = edges.find((e) => e.id === 'b-c')
    const data = labeled?.data as {labelX?: number; labelY?: number} | undefined
    expect(Number.isFinite(data?.labelX)).toBe(true)
    expect(Number.isFinite(data?.labelY)).toBe(true)
  })

  // The coordinate translation is the one piece that can silently go wrong:
  // ELK reports waypoints relative to whichever container it parked an edge in,
  // and a child node's React Flow position is relative to its parent. If either
  // offset is dropped, routed paths float off their nodes. Assert the endpoints
  // actually land on the source/target borders (in absolute flow coords).
  it('anchors routed endpoints to the source and target node borders', async () => {
    const {nodes, edges} = await runElkLayout(spec)

    const byId = new Map(nodes.map((n) => [n.id, n]))
    const absBox = (id: string): {x: number; y: number; w: number; h: number} => {
      const node = byId.get(id)!
      let x = node.position.x
      let y = node.position.y
      let parent = node.parentId
      while (parent) {
        const p = byId.get(parent)!
        x += p.position.x
        y += p.position.y
        parent = p.parentId
      }
      return {x, y, w: node.width ?? 0, h: node.height ?? 0}
    }
    // The endpoint must sit ON the node's border, not merely inside its box: a
    // point that lands in the interior would mean the route is detached. Require
    // proximity to one of the four edges (within the perpendicular span).
    const onBorder = (p: {x: number; y: number}, b: ReturnType<typeof absBox>): boolean => {
      const eps = 1.5
      const withinX = p.x >= b.x - eps && p.x <= b.x + b.w + eps
      const withinY = p.y >= b.y - eps && p.y <= b.y + b.h + eps
      const nearLeftOrRight =
        (Math.abs(p.x - b.x) <= eps || Math.abs(p.x - (b.x + b.w)) <= eps) && withinY
      const nearTopOrBottom =
        (Math.abs(p.y - b.y) <= eps || Math.abs(p.y - (b.y + b.h)) <= eps) && withinX
      return nearLeftOrRight || nearTopOrBottom
    }

    for (const edge of edges) {
      const points = (edge.data as {points: {x: number; y: number}[]}).points
      const start = points[0]!
      const end = points[points.length - 1]!
      expect(onBorder(start, absBox(edge.source))).toBe(true)
      expect(onBorder(end, absBox(edge.target))).toBe(true)
    }
  })
})
