// Tests the "Import vertices" and "Import edges for existing vertices" FE operations.

/// REQUIRE_SCRIPT load_edges_from_test_set.groovy
/// REQUIRE_SCRIPT load_vertices_from_test_set.groovy

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

project.computeUncomputed()

project.saveAs('vertex_and_edge_import_result')
