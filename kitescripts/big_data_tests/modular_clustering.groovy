// Tests the "Modular clustering" operation

/// REQUIRE_SCRIPT filter_high_degree_vertices.groovy

project = lynx.loadProject('filter_high_degree_vertices_result')

project.modularClustering(
  'name': 'modular_clusters',
  'weight': '!no weight',
  'max-iterations': '30',
  'min-increment-per-iteration': '0.001'
)

s = project.segmentations['modular_clusters']

println "modular clustering: ${s.scalars['modular_clusters']}"
