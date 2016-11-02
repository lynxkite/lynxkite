// Tests the "Approximate clustering coefficient" operation

/// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_result')

name = 'clustering_coefficient'

project.approximateClusteringCoefficient(
  'name': name,
  'bits': '8',
)

project.computeUncomputed()

