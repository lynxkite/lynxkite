// A general benchmark script with a general mix of operations.
start_time = System.currentTimeMillis()

p = new Project('createrandom')
p.newVertexSet(size: 1000000)
p.createScaleFreeRandomEdgeBundle(iterations: 5, perIterationMultiplier: 1.6, seed: 1571682864)
p.addConstantEdgeAttribute(name: 'weight', value: 1, type: 'Double')
p.exportEdgeAttributesToFile(
  path: 'UPLOAD$/randomgraph',
  link: 'edges_csv',
  attrs: 'weight',
  format: 'CSV')

p = new Project('loadandprocessrandom')
p.importVerticesAndEdgesFromSingleCSVFileset(
  dst: 'dstVertexId',
  files: 'UPLOAD$/randomgraph/data/part*',
  filter: '',
  src: 'srcVertexId',
  header: 'srcVertexId,dstVertexId,weight',
  omitted: '',
  delimiter: ',')
p.degree(name: 'degree', direction: 'all edges')
p.filterByAttributes('filterva-degree': '<500')
p.findInfocomCommunities(
  cliques_name: 'maximal_cliques',
  communities_name: 'communities',
  adjacency_threshold: 0.6,
  bothdir: 'false',
  min_cliques: 3)

c = p.segmentations['communities']
c.aggregateToSegmentation('aggregate-degree': 'average')
c.aggregateFromSegmentation(prefix: 'communities', 'aggregate-degree_average': 'max')

p.derivedEdgeAttribute(
  output: 'highest_degree',
  type: 'double',
  expr: 'Math.max(src$degree, dst$degree)')
p.aggregateEdgeAttributeGlobally(prefix: '', 'aggregate-highest_degree': 'sum')
p.edgeAttributeToDouble(attr: 'weight')
p.aggregateEdgeAttributeGlobally(prefix: '', 'aggregate-weight': 'sum')
p.aggregateVertexAttributeGlobally(
  prefix: '',
  'aggregate-communities_degree_average_max': 'std_deviation')

print p.scalars['vertex_count']
print p.scalars['communities_degree_average_max_std_deviation']
print p.scalars['highest_degree_sum']
print p.scalars['weight_sum']
print System.currentTimeMillis() - start_time
