//Tests the "Add rank attribute" operation

/// REQUIRE_SCRIPT random_attributes.groovy


project = lynx.loadProject('random_attributes')

name='ranking'

project.addRankAttribute(
  'rankattr': name,
  'keyattr': 'rnd_std_uniform',
  'order:': 'ascending'
)

histogram = project.vertexAttributes[name].histogram(
  logarithmic: true,
  precise: true)


println "$name: $histogram"

