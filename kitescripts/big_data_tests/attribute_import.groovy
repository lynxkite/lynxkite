// Test "Import vertex attributes" operation.

/// REQUIRE_SCRIPT edge_import.groovy
/// REQUIRE_SCRIPT load_attributes_from_test_set.groovy

project = lynx.loadProject('edge_import_result')

// Import vertex attrs as vertex attributes, keyed by stringID.
project.importVertexAttributes(
  table: lynx.openTable('test_vertex_attributes'),
  'id-attr': 'stringID',
  'id-column': 'id',
  'prefix': '')

project.computeUncomputed()


