// Tests the "Create edges from co-occurrance" operation

/// REQUIRE_SCRIPT maximal_cliques.groovy

project = lynx.loadProject('maximal_cliques_result')


project.discardEdges()

s = project.segmentations['maximal_cliques']

s.createEdgesFromCoOccurrence()

project.computeUncomputed()
