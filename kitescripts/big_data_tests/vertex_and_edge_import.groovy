// Tests the "Import vertices" and "Import edges for existing vertices" FE operations.
// The vertices are loaded from a table named "test_vertices"
// The edges are loaded from a table named "test_edges"

// Requires: load_test_set.groovy


project = lynx.newProject()

project.importVertices(
  'id-attr': 'id',
  table: lynx.openTable('test_vertices'))
project.importEdgesForExistingVertices(
  table: lynx.openTable('test_edges'),
  attr: 'vertex_id',
  src: 'src',
  dst: 'dst')

project.degree(direction: 'incoming edges', name: 'in_degree')
project.degree(direction: 'outgoing edges', name: 'out_degree')

println "in_degree: ${ project.vertexAttributes['in_degree'].histogram(logarithmic: true, precise: true) }"
println "out_degree: ${ project.vertexAttributes['out_degree'].histogram(logarithmic: true, precise: true) }"

project.saveAs('vertex_and_edge_import_result')
