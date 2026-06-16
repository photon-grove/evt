import {
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  type Node,
} from '@xyflow/react'
import {useEffect, useState, type ReactElement} from 'react'

import '@xyflow/react/dist/style.css'

import {runElkLayout, type LaidOutGraph} from './layout/elk'
import {edgeTypes} from './nodes/edges'
import {nodeTypes} from './nodes/nodes'
import {domainColor} from './theme/tokens'
import type {DiagramNodeData, DiagramSpec} from './types'

// fitView scales a long, single-file flow down until its whole length fits,
// which leaves nodes too small to read. Floor the initial zoom so cards stay
// legible on load; the long diagrams then exceed the viewport and the reader
// pans/scrolls through them (the minimap and controls make that obvious).
const MIN_READABLE_INITIAL_ZOOM = 0.62

interface ZoomController {
  getZoom(): number
  zoomTo(zoom: number): Promise<boolean>
}

function miniMapColor(node: Node): string {
  const data = node.data as DiagramNodeData | undefined
  return domainColor(data?.node?.domain).accent
}

function keepInitialViewReadable(instance: ZoomController): void {
  window.requestAnimationFrame(() => {
    const zoom = instance.getZoom()
    if (zoom < MIN_READABLE_INITIAL_ZOOM) {
      void instance.zoomTo(MIN_READABLE_INITIAL_ZOOM)
    }
  })
}

/**
 * Renders a single laid-out diagram. Must run on the client only — wrap in
 * {@link ClientOnly} (the DiagramViewer already does). Assumes the toolkit
 * stylesheet is present (DiagramViewer injects it).
 */
export function DiagramCanvas({spec}: {spec: DiagramSpec}): ReactElement {
  const [graph, setGraph] = useState<LaidOutGraph | null>(null)

  useEffect(() => {
    let active = true
    setGraph(null)
    runElkLayout(spec)
      .then((result) => {
        if (active) setGraph(result)
      })
      .catch(() => {
        if (active) setGraph({nodes: [], edges: []})
      })
    return () => {
      active = false
    }
  }, [spec])

  if (!graph) {
    return <div className="rfd-canvas__loading">Laying out diagram…</div>
  }

  return (
    <ReactFlowProvider>
      <ReactFlow
        nodes={graph.nodes}
        edges={graph.edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        fitView
        fitViewOptions={{padding: 0.08}}
        minZoom={0.12}
        maxZoom={1.8}
        nodesConnectable={false}
        onInit={keepInitialViewReadable}
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
  )
}
