// Simple import test.
project = new Project('loadcsv')
project.importVerticesAndEdgesFromSingleCSVFileset(
  dst: 'dstVertexId",
  files: glob,
  filter: '',
  src: 'srcVertexId',
  header: 'srcVertexId,dstVertexId,weight',
  omitted: '',
  delimiter: ',')
project.aggregateEdgeAttributeGlobally(prefix: '', 'aggregate-weight': 'count')

start_time = System.currentTimeMillis()
println "weight_count: ${ project.scalars['weight_count'] }"
println "time: ${ (System.currentTimeMillis() - start_time) / 1000 } seconds"
