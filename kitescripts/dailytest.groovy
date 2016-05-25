// A test script with a wide mix of operations.
start_time = System.currentTimeMillis()

project = lynx.newProject()
project.newVertexSet(size: 10000)
project.saveAs('dailytest') // Test saving and loading.
project = lynx.loadProject('dailytest')
project.createScaleFreeRandomEdgeBundle(iterations: 5, perIterationMultiplier: 1.6, seed: 1571682864)
project.addConstantEdgeAttribute(name: 'weight', value: 1, type: 'Double')
df = project.sql('select src_id,dst_id,edge_weight from edges')
df.write().format('com.databricks.spark.csv')
  .option('header', 'true').mode('overwrite').save('randomgraph')

project = lynx.newProject()
df = lynx.sqlContext.read().format('com.databricks.spark.csv')
  .option('header', 'true').load('randomgraph')
table = lynx.saveAsTable(df, 'randomgraph_table')
project.importVerticesAndEdgesFromASingleTable(
  table: table,
  dst: 'dst_id',
  src: 'src_id')
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
project.edgeAttributeToDouble(attr: 'edge_weight')
project.aggregateEdgeAttributeGlobally(prefix: '', 'aggregate-edge_weight': 'sum')
project.aggregateVertexAttributeGlobally(
  prefix: '',
  'aggregate-communities_degree_average_max': 'std_deviation')
project.renameScalar(from: 'communities_degree_average_max_std_deviation', to: 'cdamsd')

println "vertex_count: ${ project.scalars['vertex_count'] }"
println "cdamsd: ${ project.scalars['cdamsd'] }"
println "highest_degree_sum: ${ project.scalars['highest_degree_sum'] }"
println "edge_weight_sum: ${ project.scalars['edge_weight_sum'] }"
println "time: ${ (System.currentTimeMillis() - start_time) / 1000 } seconds"
