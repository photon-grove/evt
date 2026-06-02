# @photon-grove/react-flow-diagrams

A local React Flow + ELK toolkit used by the `evt` docs site to render
interactive architecture diagrams from typed semantic data.

Consumers author `DiagramSpec[]` objects. The toolkit handles layout, node
rendering, routed edges, a sidebar picker, controls, minimap, legend, and theme
tokens. This keeps diagrams reviewable in code without hand-placed coordinates.

```tsx
import {DiagramViewer, type DiagramSpec} from '@photon-grove/react-flow-diagrams'

const diagrams: DiagramSpec[] = [
  {
    id: 'overview',
    title: 'System overview',
    layout: {direction: 'RIGHT'},
    nodes: [
      {id: 'api', kind: 'service', label: 'API', domain: 'api'},
      {id: 'event-log', kind: 'datastore', label: 'event-log', domain: 'data'},
    ],
    edges: [{id: 'commit', source: 'api', target: 'event-log', variant: 'event'}],
  },
]

export function Docs() {
  return <DiagramViewer diagrams={diagrams} title="evt" />
}
```

Import `@xyflow/react/dist/style.css` once at the docs app entrypoint. The
toolkit injects its own visual stylesheet.
