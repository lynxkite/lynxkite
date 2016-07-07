// Test filtering out high-degree vertices.

/// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_result')

project.degree(direction: 'all edges', name: 'degree')
project.filterByAttributes('filterva-degree': '< 5000')

project.computeUncomputed()

project.saveAs('filter_high_degree_vertices_result')
