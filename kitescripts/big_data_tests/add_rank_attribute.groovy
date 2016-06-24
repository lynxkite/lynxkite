//Tests the "Add rank attribute" operation

/// REQUIRE_SCRIPT random_attributes.groovy


project = lynx.loadProject('random_attributes')

name = 'ranking'

project.addRankAttribute(
  'rankattr': name,
  'keyattr': 'rnd_std_uniform',
  'order:': 'ascending'
)

project.vertexAttributes[name].computeAndPrintHistogram(
        'name': name,
        logarithmic: true)

