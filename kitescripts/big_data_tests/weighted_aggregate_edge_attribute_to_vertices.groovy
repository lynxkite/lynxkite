// Tests the "Weighted aggregate edge attribute to vertices" operation

/// REQUIRE_SCRIPT random_attributes.groovy

project = lynx.loadProject('random_attributes')

project.weightedAggregateEdgeAttributeToVertices(
  'prefix': '',
  'weight': 'rnd_std_uniform',
  'direction': 'all edges' ,
  'aggregate-rnd_std_normal': 'weighted_average'
)

project.computeUncomputed()
