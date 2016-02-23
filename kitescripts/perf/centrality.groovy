// Centrality benchmark.
start_time = System.currentTimeMillis()

project = lynx.newProject()
project.newVertexSet(size: '500000')
project.createRandomEdgeBundle(degree: '30', seed: '-1860004248')
project.centrality(algorithm: 'Harmonic', bits: '8', maxDiameter: '10', name: 'centrality')

println "centrality distribution: ${ project.vertexAttributes['centrality'].histogram() }"
println "time: ${ (System.currentTimeMillis() - start_time) / 1000 } seconds"
