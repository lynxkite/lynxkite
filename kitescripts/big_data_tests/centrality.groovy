// Centrality benchmark.

/// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_result')

project.centrality(algorithm: 'Harmonic', bits: '4', maxDiameter: '5', name: 'centrality')

centrality_histogram = project.vertexAttributes['centrality'].histogram(
  logarithmic: true,
  precise: true)
