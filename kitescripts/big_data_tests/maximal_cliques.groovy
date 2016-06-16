// Tests the "Maximal cliques" operation

/// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_result')


project.maximalCliques(
  'bothdir': 'true',
  'name': 'maximal_cliques',
  'min': '3'
)

s = project.segmentations['maximal_cliques']

println "max cliques: ${s.scalars['vertex_count']}"

