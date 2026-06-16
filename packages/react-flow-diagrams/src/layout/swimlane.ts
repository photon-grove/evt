import type {Edge, Node} from '@xyflow/react'

import {nodeSize} from '../theme/tokens'
import type {DiagramNode, DiagramNodeData, DiagramSpec, LaneNodeData} from '../types'
import {baseEdge} from './edge'
import type {LaidOutGraph} from './elk'

// Geometry constants for the swimlane grid. Lanes are full-width horizontal
// bands; the flow runs left-to-right across columns (one column per step).
const LANE_LABEL_GUTTER = 136 // left strip reserved for the lane label
const COLUMN_GAP = 64 // horizontal space between step columns
const COLUMN_PAD = 30 // gap after the gutter and before the right edge
const LANE_VPAD = 28 // breathing room above/below the card in a lane

// z-order: bands sit behind everything, edges above the bands, cards on top so
// connector lines never cover a card face. React Flow sorts node/edge wrappers
// by these explicit zIndex values within the shared viewport stacking context.
const LANE_Z = 0
const EDGE_Z = 5
const CARD_Z = 10

/**
 * Lay a spec out as horizontal swimlanes. Each node sits in the row named by its
 * `lane`; its column is the node's longest-path depth from a source, so the flow
 * reads left-to-right and every edge points forward. Returns lane band nodes
 * (rendered behind, type `lane`) followed by the cards, each parented to its
 * band so React Flow keeps the band underneath.
 *
 * Unlike {@link runElkLayout} this is synchronous and deterministic — there is
 * no edge routing; React Flow draws a smoothstep between handles.
 */
export function runSwimlaneLayout(spec: DiagramSpec): LaidOutGraph {
  const lanes = spec.layout?.lanes ?? []
  const laneIndex = new Map(lanes.map((lane, i) => [lane.id, i]))
  const fallbackLane = lanes[0]?.id ?? ''
  const laneOf = (id?: string): string => (id && laneIndex.has(id) ? id : fallbackLane)

  const depth = column(spec)
  const columns = spec.nodes.reduce((max, n) => Math.max(max, depth.get(n.id) ?? 0), 0) + 1

  // Card slot is the tallest card; each column is as wide as its widest card.
  const colWidth = Array.from({length: columns}, () => 0)
  let tallest = 0
  for (const node of spec.nodes) {
    const {width, height} = nodeSize(node.kind)
    const c = depth.get(node.id) ?? 0
    colWidth[c] = Math.max(colWidth[c]!, width)
    tallest = Math.max(tallest, height)
  }

  // Left edge of each column in absolute (flow) coordinates.
  const colX: number[] = []
  let cursor = LANE_LABEL_GUTTER + COLUMN_PAD
  for (let c = 0; c < columns; c++) {
    colX[c] = cursor
    cursor += colWidth[c]! + COLUMN_GAP
  }
  const totalWidth = cursor - COLUMN_GAP + COLUMN_PAD

  // Group cards into (lane row, column) cells, preserving spec order. Several
  // nodes can share a cell — same lane at the same longest-path depth (a branch
  // handled by one actor, or two source nodes in one lane) — so cards in a cell
  // stack vertically instead of rendering on top of each other.
  const rowOf = (node: DiagramNode): number => laneIndex.get(laneOf(node.lane)) ?? 0
  const cellKey = (r: number, c: number): string => `${r}:${c}`
  const cells = new Map<string, DiagramNode[]>()
  for (const node of spec.nodes) {
    const key = cellKey(rowOf(node), depth.get(node.id) ?? 0)
    cells.set(key, [...(cells.get(key) ?? []), node])
  }

  // Each lane is tall enough for its busiest cell; lanes stack with cumulative
  // tops so a crowded lane never clips into the next.
  const rowCount = Math.max(lanes.length, 1)
  const rowHeight = Array.from({length: rowCount}, (_, r) => {
    let stack = 1
    for (let c = 0; c < columns; c++) {
      stack = Math.max(stack, cells.get(cellKey(r, c))?.length ?? 0)
    }
    return stack * tallest + (stack + 1) * LANE_VPAD
  })
  const laneTop: number[] = []
  rowHeight.reduce((top, height, r) => {
    laneTop[r] = top
    return top + height
  }, 0)

  const laneNodes: Node<LaneNodeData>[] = lanes.map((lane, i) => ({
    id: laneId(lane.id),
    type: 'lane',
    position: {x: 0, y: laneTop[i]!},
    width: totalWidth,
    height: rowHeight[i]!,
    data: {label: lane.label, index: i},
    selectable: false,
    draggable: false,
    zIndex: LANE_Z,
    style: {width: totalWidth, height: rowHeight[i]!},
  }))

  const cardNodes: Node<DiagramNodeData>[] = spec.nodes.map((node) => {
    const {width, height} = nodeSize(node.kind)
    const r = rowOf(node)
    const c = depth.get(node.id) ?? 0
    const bucket = cells.get(cellKey(r, c)) ?? [node]
    const slot = bucket.indexOf(node)
    const stack = bucket.length

    // Center the stack of cards within the lane band, then drop this card into
    // its slot. Positions are relative to the parent band (extent: 'parent').
    const stackHeight = stack * tallest + (stack - 1) * LANE_VPAD
    const stackTop = (rowHeight[r]! - stackHeight) / 2
    const x = colX[c]! + (colWidth[c]! - width) / 2
    const y = stackTop + slot * (tallest + LANE_VPAD) + (tallest - height) / 2

    return {
      id: node.id,
      type: node.kind,
      parentId: laneId(laneOf(node.lane)),
      extent: 'parent' as const,
      position: {x, y},
      data: {node, direction: 'RIGHT'},
      width,
      height,
      style: {width, height},
      zIndex: CARD_Z,
    }
  })

  const edges: Edge[] = spec.edges.map((edge) => ({
    ...baseEdge(edge),
    type: 'smoothstep',
    zIndex: EDGE_Z,
  }))

  return {nodes: [...laneNodes, ...cardNodes] as Node<DiagramNodeData>[], edges}
}

const laneId = (id: string): string => `lane:${id}`

/**
 * Longest-path layering: each node's column is the longest chain of edges
 * reaching it from a source. Linear flows collapse to 0,1,2,…; branches share a
 * column when they're the same distance from the start. Falls back to spec order
 * for nodes left unranked by a cycle or disconnected island.
 */
function column(spec: DiagramSpec): Map<string, number> {
  const depth = new Map<string, number>()
  const indeg = new Map<string, number>()
  const adj = new Map<string, string[]>()

  for (const node of spec.nodes) indeg.set(node.id, 0)
  for (const edge of spec.edges) {
    indeg.set(edge.target, (indeg.get(edge.target) ?? 0) + 1)
    adj.set(edge.source, [...(adj.get(edge.source) ?? []), edge.target])
  }

  const queue = spec.nodes.filter((n) => (indeg.get(n.id) ?? 0) === 0).map((n) => n.id)
  for (const id of queue) depth.set(id, 0)

  let head = 0
  while (head < queue.length) {
    const id = queue[head++]!
    const d = depth.get(id) ?? 0
    for (const next of adj.get(id) ?? []) {
      depth.set(next, Math.max(depth.get(next) ?? 0, d + 1))
      indeg.set(next, (indeg.get(next) ?? 0) - 1)
      if ((indeg.get(next) ?? 0) === 0) queue.push(next)
    }
  }

  spec.nodes.forEach((node, i) => {
    if (!depth.has(node.id)) depth.set(node.id, i)
  })

  return depth
}
