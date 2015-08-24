// Finds cliques in a small random graph.
project = new Project(project)
project.newVertexSet(size: 10)
project.createRandomEdgeBundle(degree: 5, seed: seed)
project.maximalCliques(name: 'maximal_cliques', bothdir: false, min: 2)

cliques = project.segmentations['maximal_cliques']
cliques.aggregateFromSegmentation(prefix: 'maximal_cliques', 'aggregate-size': 'average')

project.aggregateVertexAttributeGlobally(
  prefix: '',
  'aggregate-maximal_cliques_size_average': 'average')

start_time = System.currentTimeMillis()
println "maximal_cliques_size_average_average: ${ project.scalars['maximal_cliques_size_average_average'] }"
println "time: ${ (System.currentTimeMillis() - start_time) / 1000 } seconds"
