// Tests the "Triadic Closure" FE operation

/// REQUIRE_SCRIPT filter_high_degree_vertices.groovy

// filtering high degree vertices before closure to prevent unmanageable size output
project = lynx.loadProject('filter_high_degree_vertices_result')

// more filtering
project.filterByAttributes('filterva-degree': '< 100')

// create random edge attribute for testing the pulling of edge attributes
project.addRandomEdgeAttribute(
        name: 'attr',
        dist: 'Standard Uniform',
        seed: '123421')

project.triadicClosure()

project.computeUncomputed()
