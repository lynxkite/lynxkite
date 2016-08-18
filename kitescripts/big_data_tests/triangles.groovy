// Tests the "Triangles" operation.

/// REQUIRE_SCRIPT filter_high_degree_vertices.groovy

project = lynx.loadProject('filter_high_degree_vertices_result')

project.filterByAttributes('filterva-degree': '< 1000')

project.triangles(
        'bothdir': 'false',
        'name': 'triangles',
)

project.saveAs('triangles_result')

project.computeUncomputed()
