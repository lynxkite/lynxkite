// Tests the "Import vertices and edges from a single table" FE operation.

/// REQUIRE_SCRIPT load_edges_from_test_set.groovy

project = lynx.newProject()

project.importVerticesAndEdgesFromASingleTable(
  table: lynx.openTable('test_edges'),
  src: 'src',
  dst: 'dst')

project.degree(direction: 'incoming edges', name: 'in_degree')
project.degree(direction: 'outgoing edges', name: 'out_degree')


project.vertexAttributes['in_degree'].computeAndPrintHistogram(name: 'in_degree', logarithmic: true)
project.vertexAttributes['out_degree'].computeAndPrintHistogram(name: 'out_degree', logarithmic: true)

project.saveAs('edge_import_result')
