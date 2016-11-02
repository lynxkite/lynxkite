// Tests the "Enumerate triangles" operation.

/// REQUIRE_SCRIPT filter_high_degree_vertices.groovy

project = lynx.loadProject('filter_high_degree_vertices_result')

project.filterByAttributes('filterva-degree': '< 1000')

project.enumeratetriangles(
        'bothdir': 'false',
        'name': 'triangles',
)

project.saveAs('enumerate_triangles_result')

project.computeUncomputed()
