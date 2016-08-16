// Add useful global and local constants to random_attributes to make my
// life easier

/// REQUIRE_SCRIPT random_attributes.groovy

project = lynx.loadProject('random_attributes')

project.addConstantVertexAttribute(
  'name': 'one',
  'value': '1.0',
  'type': 'Double'
)

project.aggregateVertexAttributeGlobally(
  'prefix': '',
  'aggregate-one': 'sum'
)

project.renameScalar(
  'from': 'one_sum',
  'to': 'number_of_vertices'
)

project.saveAs('random_attributes_with_constants_result')

