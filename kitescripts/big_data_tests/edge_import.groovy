// Tests the "Import vertices and edges from a single table" FE operation.
// The edges are loaded from a table named "test_edges"

// Requires: load_test_set.groovy

project = lynx.newProject()

project.importVerticesAndEdgesFromASingleTable(
  table: lynx.openTable('test_edges'),
  src: 'src',
  dst: 'dst')

project.degree(direction: 'incoming edges', name: 'in_degree')
project.degree(direction: 'outgoing edges', name: 'out_degree')

println "in_degree: ${ project.vertexAttributes['in_degree'].histogram(logarithmic: true, precise: true) }"
println "out_degree: ${ project.vertexAttributes['out_degree'].histogram(logarithmic: true, precise: true) }"

project.saveAs('edge_import_result')
