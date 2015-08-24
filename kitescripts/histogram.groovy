// Benchmark for a large histogram calculation.
project = new Project('random')
project.newVertexSet(size: 100)
project.addGaussianVertexAttribute(name: 'random', seed: 1571682864)
project.aggregateVertexAttributeGlobally(prefix: '', 'aggregate-random': 'average')

println "vertex_count: ${ project.scalars['vertex_count'] }"
start_time = System.currentTimeMillis()
println "histogram: ${ project.vertexAttributes['random'].histogram }"
println "time: ${ (System.currentTimeMillis() - start_time) / 1000 } seconds"
