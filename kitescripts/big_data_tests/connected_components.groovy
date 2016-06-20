// Tests the "Connected components" operation

/// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_result')

project.connectedComponents(
  'directions': 'ignore directions',
  'name': 'connected_components',
)

s = project.segmentations['connected_components']

println "connected_components: ${s.scalars['vertex_count']}"



