import {
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  type Node,
  type ReactFlowInstance,
} from '@xyflow/react'
import {useCallback, useEffect, useRef, useState, type ReactElement} from 'react'

import '@xyflow/react/dist/style.css'

import {runElkLayout, type LaidOutGraph} from './layout/elk'
import {runSwimlaneLayout} from './layout/swimlane'
import {edgeTypes} from './nodes/edges'
import {nodeTypes} from './nodes/nodes'
import {domainColor} from './theme/tokens'
import type {DiagramNodeData, DiagramSpec} from './types'

// Diagrams load at 100% so every card is legible immediately, anchored to where
// the flow begins (the left edge for horizontal/swimlane flows, the top for
// vertical ones). Longer diagrams overflow the viewport and the reader pans /
// scrolls — or zooms out — with the minimap and controls. The cross axis is
// centered when the content fits, so short diagrams sit balanced.
const INITIAL_ZOOM = 1
const VIEW_PAD = 28

function miniMapColor(node: Node): string {
  const data = node.data as DiagramNodeData | undefined
  return domainColor(data?.node?.domain).accent
}

/** Anchor the initial viewport at {@link INITIAL_ZOOM} to the flow's start. */
function applyInitialView(
  instance: ReactFlowInstance<Node<DiagramNodeData>>,
  width: number,
  height: number,
  horizontal: boolean
): void {
  const nodes = instance.getNodes()
  if (nodes.length === 0) return

  // Instance method (not the standalone helper) so parented swimlane cards
  // resolve against the node lookup and report correct absolute bounds.
  const bounds = instance.getNodesBounds(nodes)
  const zoom = INITIAL_ZOOM
  const contentW = bounds.width * zoom
  const contentH = bounds.height * zoom

  // Start = pin the flow's leading edge near the corner; center = balance the
  // cross axis when the content already fits, else also pin to the start.
  const start = (origin: number): number => VIEW_PAD - origin * zoom
  const center = (avail: number, content: number, origin: number): number =>
    content <= avail - 2 * VIEW_PAD ? (avail - content) / 2 - origin * zoom : start(origin)

  const x = horizontal ? start(bounds.x) : center(width, contentW, bounds.x)
  const y = horizontal ? center(height, contentH, bounds.y) : start(bounds.y)

  instance.setViewport({x, y, zoom})
}

/**
 * Renders a single laid-out diagram. Must run on the client only — wrap in
 * {@link ClientOnly} (the DiagramViewer already does). Assumes the toolkit
 * stylesheet is present (DiagramViewer injects it).
 */
export function DiagramCanvas({spec}: {spec: DiagramSpec}): ReactElement {
  const [graph, setGraph] = useState<LaidOutGraph | null>(null)
  const wrapperRef = useRef<HTMLDivElement>(null)

  // A spec with lanes uses the synchronous swimlane grid; everything else goes
  // through ELK. Dragging is disabled for swimlanes so cards stay in their rows.
  const isSwimlane = (spec.layout?.lanes?.length ?? 0) > 0
  // Swimlanes and left-to-right flows scroll horizontally; vertical flows scroll
  // down. This picks which axis the initial view pins to its start.
  const horizontal = isSwimlane || (spec.layout?.direction ?? 'RIGHT') === 'RIGHT'

  const handleInit = useCallback(
    (instance: ReactFlowInstance<Node<DiagramNodeData>>) => {
      const el = wrapperRef.current
      if (!el) return
      window.requestAnimationFrame(() =>
        applyInitialView(instance, el.clientWidth, el.clientHeight, horizontal)
      )
    },
    [horizontal]
  )

  useEffect(() => {
    let active = true
    setGraph(null)
    const layout = isSwimlane ? Promise.resolve(runSwimlaneLayout(spec)) : runElkLayout(spec)
    layout
      .then((result) => {
        if (active) setGraph(result)
      })
      .catch(() => {
        if (active) setGraph({nodes: [], edges: []})
      })
    return () => {
      active = false
    }
  }, [spec, isSwimlane])

  if (!graph) {
    return <div className="rfd-canvas__loading">Laying out diagram…</div>
  }

  return (
    <div ref={wrapperRef} style={{width: '100%', height: '100%'}}>
      <ReactFlowProvider>
        <ReactFlow
          nodes={graph.nodes}
          edges={graph.edges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          defaultViewport={{x: VIEW_PAD, y: VIEW_PAD, zoom: INITIAL_ZOOM}}
          minZoom={0.12}
          maxZoom={1.8}
          nodesConnectable={false}
          nodesDraggable={!isSwimlane}
          onInit={handleInit}
          proOptions={{hideAttribution: false}}
        >
          <Background
            variant={BackgroundVariant.Dots}
            gap={22}
            size={1.1}
            color="var(--rfd-edge-muted)"
          />
          <MiniMap
            pannable
            zoomable
            nodeColor={miniMapColor}
            nodeStrokeWidth={2}
            maskColor="rgba(15,23,42,0.08)"
          />
          <Controls showInteractive={false} />
        </ReactFlow>
      </ReactFlowProvider>
    </div>
  )
}
