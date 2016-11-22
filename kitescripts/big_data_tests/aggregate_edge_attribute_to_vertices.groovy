// Tests the "Weighted aggregate edge attribute to vertices" operation

/// REQUIRE_SCRIPT random_attributes.groovy

project = lynx.loadProject('random_attributes')

project.aggregateEdgeAttributeToVertices(
  'prefix': '',
  'direction': 'all edges' ,
  'aggregate-rnd_std_normal': 'most_common'
)

project.computeUncomputed()
