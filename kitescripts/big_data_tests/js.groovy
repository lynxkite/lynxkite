// Tests JavaScript execution performance.

// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_results')
project.vertexAttributeToDouble(attr: 'ordinal')
project.derivedVertexAttribute(
  output: 'x',
  type: 'double',
  expr: 'ordinal * ordinal')

println "vertices: $vertices"
println "x: ${ project.vertexAttributes['x'].histogram() }"
