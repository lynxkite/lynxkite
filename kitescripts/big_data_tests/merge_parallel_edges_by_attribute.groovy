// Tests the "Merge parallel edges by attribute" operation

/// REQUIRE_SCRIPT random_attributes.groovy

project = lynx.loadProject('random_attributes')

project.derivedEdgeAttribute(
  'output': 'label',
  'type': 'double',
  'expr': 'Math.floor(rnd_std_uniform*2)'
)

project.mergeParallelEdgesByAttribute(
  'key': 'label',
  'aggregate-label': 'average'
)

project.edgeAttributes['label_average']
  .computeAndPrintHistogram(
    name: 'label_average',
  )


println "vertices: ${project.scalars['vertex_count']} edges ${project.scalars['edge_count']}"
