/// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_result')

project.findVertexColoring(name: 'color')

println "colors: ${ project.vertexAttributes['color'].histogram(logarithmic: false, precise: true) }"

/**
 * Created by huncros on 2016.05.13..
 */
