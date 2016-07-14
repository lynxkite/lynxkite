// Tests the "Maximal cliques" operation

/// REQUIRE_SCRIPT filter_high_degree_vertices.groovy

project = lynx.loadProject('filter_high_degree_vertices_result')

project.maximalCliques(
  'bothdir': 'true',
  'name': 'maximal_cliques',
  'min': '3'
)

project.saveAs('maximal_cliques_result')

project.computeUncomputed()

