// Tests the "Embeddedness" operation.

/// REQUIRE_SCRIPT filter_high_degree_vertices_1000.groovy

project = lynx.loadProject('filter_high_degree_vertices_1000_result')

project.embeddedness(
        'name': 'embeddedness',
)

project.saveAs('embeddedness_result')

project.computeUncomputed()
