// Tests the "Merge vertices by attribute" operation

/// REQUIRE_SCRIPT random_attributes_with_constants.groovy

project = lynx.loadProject('random_attributes_with_constants_result')


project.derivedVertexAttribute(
  'output': 'label',
  'type': 'double',
  'expr': 'Math.floor(rnd_std_uniform*number_of_vertices*0.01)'
)

project.mergeVerticesByAttribute(
  'key': 'label',
  'aggregate-rnd_std_normal': 'average'
)

project.computeUncomputed()
