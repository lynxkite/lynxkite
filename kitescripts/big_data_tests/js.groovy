// Tests JavaScript execution performance.

/// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_result')

project.addRandomVertexAttribute(
  name: 'random',
  dist: 'Standard Uniform',
  seed: 20160419
)
project.derivedVertexAttribute(
  output: 'x',
  type: 'double',
  expr: 'random * random')

println "x: ${ project.vertexAttributes['x'].histogram(precise: true) }"
