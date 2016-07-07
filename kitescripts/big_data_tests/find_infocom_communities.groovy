// Tests the "Find infocom communities" operation

/// REQUIRE_SCRIPT maximal_cliques.groovy

project = lynx.loadProject('maximal_cliques_result')

project.findInfocomCommunities(
  'cliques_name': 'maximal_cliques',
  'communities_name': 'communities',
  'bothdir': 'false',
  'min_cliques': '3',
  'adjacency_threshold': '0.6'
)

project.computeUncomputed()

