// Tests JavaScript execution performance.

/// REQUIRE_SCRIPT random_attributes.groovy

project = lynx.loadProject('random_attributes')

project.derivedEdgeAttribute(
  output: 'x',
  type: 'double',
  expr: 'rnd_std_uniform * rnd_std_uniform')

project.edgeAttributes['x'].computeAndPrintHistogram(name: 'x', precise: true)
