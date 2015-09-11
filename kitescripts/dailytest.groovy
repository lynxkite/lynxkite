// A benchmark/test script with a wide mix of operations.
start_time = System.currentTimeMillis()

project = lynx.project('createrandom')
project.newVertexSet(size: 1000000)
project.createScaleFreeRandomEdgeBundle(iterations: 5, perIterationMultiplier: 1.6, seed: 1571682864)
project.addConstantEdgeAttribute(name: 'weight', value: 1, type: 'Double')
project.exportEdgeAttributesToFile(
  path: 'UPLOAD$/randomgraph',
  link: 'edges_csv',
  attrs: 'weight',
  format: 'CSV')

project = lynx.project('loadandprocessrandom')
project.importVerticesAndEdgesFromSingleCSVFileset(
  dst: 'dstVertexId',
  files: 'UPLOAD$/randomgraph/data/part*',
  filter: '',
  src: 'srcVertexId',
  header: 'srcVertexId,dstVertexId,weight',
  omitted: '',
  allow_corrupt_lines: 'no',
  delimiter: ',')
project.degree(name: 'degree', direction: 'all edges')
project.filterByAttributes('filterva-degree': '<500')
project.findInfocomCommunities(
  cliques_name: 'maximal_cliques',
  communities_name: 'communities',
  adjacency_threshold: 0.6,
  bothdir: 'false',
  min_cliques: 3)

communities = project.segmentations['communities']
communities.aggregateToSegmentation('aggregate-degree': 'average')
communities.aggregateFromSegmentation(prefix: 'communities', 'aggregate-degree_average': 'max')

project.derivedEdgeAttribute(
  output: 'highest_degree',
  type: 'double',
  expr: 'Math.max(src$degree, dst$degree)')
project.aggregateEdgeAttributeGlobally(prefix: '', 'aggregate-highest_degree': 'sum')
project.edgeAttributeToDouble(attr: 'weight')
project.aggregateEdgeAttributeGlobally(prefix: '', 'aggregate-weight': 'sum')
project.aggregateVertexAttributeGlobally(
  prefix: '',
  'aggregate-communities_degree_average_max': 'std_deviation')
project.renameScalar(from: 'communities_degree_average_max_std_deviation', to: 'cdamsd')

println "vertex_count: ${ project.scalars['vertex_count'] }"
println "cdamsd: ${ project.scalars['cdamsd'] }"
println "highest_degree_sum: ${ project.scalars['highest_degree_sum'] }"
println "weight_sum: ${ project.scalars['weight_sum'] }"
println "time: ${ (System.currentTimeMillis() - start_time) / 1000 } seconds"
