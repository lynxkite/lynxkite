// Centrality benchmark.

// Requires: edge_import.groovy

project = lynx.loadProject('edge_import_result')
project.centrality(algorithm: 'Harmonic', bits: '8', maxDiameter: '10', name: 'centrality')

centrality_histogram = project.vertexAttributes['centrality'].histogram(
  logarithmic: true,
  precise: true)
