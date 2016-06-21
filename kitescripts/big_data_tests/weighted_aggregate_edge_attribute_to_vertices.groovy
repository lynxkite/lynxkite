// Tests the "Weighted aggregate edge attribute to vertices" operation

/// REQUIRE_SCRIPT random_attributes.groovy

project = lynx.loadProject('random_attributes')

project.weightedAggregateEdgeAttributeToVertices(
  'prefix': '',
  'weight': 'rnd_std_uniform',
  'direction': 'all edges' ,
  'aggregate-rnd_std_normal': 'weighted_average'
)


histogram = project
  .vertexAttributes['rnd_std_normal_weighted_average_by_rnd_std_uniform']
  .histogram(
    logarithmic: true,
    precise: true
  )

println "Weighted aggregate edge attribute to vertices: $histogram"




