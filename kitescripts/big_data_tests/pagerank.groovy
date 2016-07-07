// Tests the "PageRank" FE operation with both weights and no weigths.

/// REQUIRE_SCRIPT random_attributes.groovy

project = lynx.loadProject('random_attributes')

project.pagerank(
  name: 'page_rank_no_weights',
  weights: '!no weight',
  iterations: '5',
  damping: '0.85')

// PageRank ignores non-positive weights so let's use Standard Uniform distribution.
project.pagerank(
  name: 'page_rank_weights',
  weights: 'rnd_std_uniform',
  iterations: '5',
  damping: '0.85')

project.computeUncomputed()
