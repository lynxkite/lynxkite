// Test filtering out high-degree vertices.

/// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_result')

project.degree(direction: 'all edges', name: 'degree')
project.filterByAttributes('filterva-degree': '< 5000')

println "vertex_count: ${ project.scalars['vertex_count'] }"
println "edge_count: ${ project.scalars['edge_count'] }"

project.saveAs('filter_high_degree_vertices_result')
