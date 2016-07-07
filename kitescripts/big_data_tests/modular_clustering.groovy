// Tests the "Modular clustering" operation

/// REQUIRE_SCRIPT filter_high_degree_vertices.groovy

project = lynx.loadProject('filter_high_degree_vertices_result')

project.modularClustering(
  'name': 'modular_clusters',
  'weights': '!no weight',
  'max-iterations': '30',
  'min-increment-per-iteration': '0.001'
)

project.computeUncomputed()
