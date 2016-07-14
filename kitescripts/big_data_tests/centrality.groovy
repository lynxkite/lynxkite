// Centrality benchmark.

/// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_result')

name = 'centrality'

project.centrality(algorithm: 'Harmonic', bits: '4', maxDiameter: '5', name: name)
project.computeUncomputed()

