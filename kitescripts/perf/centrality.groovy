// Centrality benchmark.
start_time = System.currentTimeMillis()

project = lynx.newProject()
project.newVertexSet(size: '500000')
project.createRandomEdgeBundle(degree: '30', seed: '-1860004248')
project.centrality(algorithm: 'Harmonic', bits: '8', maxDiameter: '10', name: 'centrality')

centrality_histogram = project.vertexAttributes['centrality'].histogram(
  logarithmic: true,
  precise: true)
println "centrality distribution: ${ centrality_histogram }"
println "time: ${ (System.currentTimeMillis() - start_time) / 1000 } seconds"
