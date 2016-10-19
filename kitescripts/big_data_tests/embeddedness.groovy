// Tests the "Embeddedness" operation.

/// REQUIRE_SCRIPT filter_high_degree_vertices.groovy

project = lynx.loadProject('filter_high_degree_vertices_result')

project.embeddedness(
        'name': 'embeddedness',
)

project.saveAs('embeddedness_result')

project.computeUncomputed()
