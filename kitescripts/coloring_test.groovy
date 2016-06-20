// Tests "Find vertex coloring" operation

/// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_result')

project.findVertexColoring('name': 'color')

println "Find vertex coloring histogram: ${ project.vertexAttributes['color'].histogram() }"

