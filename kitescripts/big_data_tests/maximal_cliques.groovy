// Tests the "Maximal cliques" operation

/// REQUIRE_SCRIPT filter_high_degree_vertices.groovy

project = lynx.loadProject('filter_high_degree_vertices_result')

project.maximalCliques(
  'bothdir': 'yes',
  'name': 'maximal_cliques',
  'min': '3'
)

s = project.segmentations['maximal_cliques']

println "segmentation vertices: ${s.scalars['vertex_count']} segmentation edges: ${s.scalars['edge_count']}"

