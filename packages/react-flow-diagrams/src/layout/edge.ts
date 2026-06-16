import {MarkerType, type Edge} from '@xyflow/react'

import {EDGE_STYLE} from '../theme/tokens'
import type {DiagramEdge} from '../types'

/**
 * The React Flow edge props shared by every layout: stroke styling, the arrow
 * marker, handle ids, and the opaque legible label chip. Routing (the edge
 * `type` and any waypoint `data`) is layout-specific and added by the caller —
 * the ELK layout follows routed waypoints, the swimlane layout lets React Flow
 * draw a smoothstep between handles.
 */
export function baseEdge(edge: DiagramEdge): Edge {
  const variant = edge.variant ?? 'data'
  const style = EDGE_STYLE[variant]

  return {
    id: edge.id,
    source: edge.source,
    target: edge.target,
    sourceHandle: 'out',
    targetHandle: 'in',
    label: edge.label,
    animated: edge.animated ?? style.animated,
    style: {stroke: style.stroke, strokeWidth: style.strokeWidth, strokeDasharray: style.dash},
    markerEnd:
      variant === 'dependency'
        ? undefined
        : {type: MarkerType.ArrowClosed, color: style.stroke, width: 15, height: 15},
    // Opaque, bordered chip so a label stays legible even when it lands on or
    // near another edge.
    labelBgStyle: {
      fill: 'var(--rfd-card)',
      fillOpacity: 1,
      stroke: 'var(--rfd-border)',
      strokeWidth: 1,
    },
    labelStyle: {fill: 'var(--rfd-ink)', fontWeight: 600, fontSize: 11},
    labelBgPadding: [7, 3],
    labelBgBorderRadius: 6,
  }
}
