// Tests the "PageRank" FE operation with both weights and no weigths.

/// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_result')
project.addReversedEdges()

// PageRank ignores non-positive weights so let's use Standard Uniform distribution.
project.addRandomEdgeAttribute(
  name: 'w',
  dist: 'Standard Uniform',
  seed: '1234')

project.pagerank(
  name: 'page_rank_no_weights',
  weights: '!no weight',
  iterations: '5',
  damping: '0.85')
project.pagerank(
  name: 'page_rank_weights',
  weights: 'w',
  iterations: '5',
  damping: '0.85')

println "page_rank_no_weights: ${ project.vertexAttributes['page_rank_no_weights'].histogram(logarithmic: true, precise: true) }"
println "page_rank_weights: ${ project.vertexAttributes['page_rank_weights'].histogram(logarithmic: true, precise: true) }"
