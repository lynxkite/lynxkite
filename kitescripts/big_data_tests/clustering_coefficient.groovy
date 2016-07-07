// Tests the "Clustering coefficient" operation

/// REQUIRE_SCRIPT filter_high_degree_vertices.groovy

project = lynx.loadProject('filter_high_degree_vertices_result')

name = 'clustering_coefficient'

project.clusteringCoefficient(
  'name': name,
)

project.computeUncomputed()



