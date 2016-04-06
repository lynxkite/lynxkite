// Tests the "Import vertices" and "Import edges for existing vertices" FE operations.
// The vertices are loaded from edges.csv of the testSet into a table "test_vertices"
// The edges are loaded from edges.csv of the testSet into a table "test_edges"

testSet = params.testSet ?: 'fake_westeros_100k'
edgePath = lynx.resolvePath('S3$/lynxkite-test-data/' + testSet + '/edges.csv')
vertexPath = lynx.resolvePath('S3$/lynxkite-test-data/' + testSet + '/vertices.csv')

project = lynx.newProject()

vertexDF = lynx.sqlContext.read()
  .format('com.databricks.spark.csv')
  .option('header', 'true')
  .load(vertexPath)
project.importVertices(
  'id-attr': 'id',
  table: lynx.saveAsTable(vertexDF, 'test_vertices'))

edgeDF = lynx.sqlContext.read()
  .format('com.databricks.spark.csv')
  .option('header', 'true')
  .load(edgePath)
project.importEdgesForExistingVertices(
  table: lynx.saveAsTable(edgeDF, 'test_edges'),
  attr: 'vertex_id',
  src: 'src',
  dst: 'dst')

project.degree(direction: 'incoming edges', name: 'in_degree')
project.degree(direction: 'outgoing edges', name: 'out_degree')

println "in_degree: ${ project.vertexAttributes['in_degree'].histogram(logarithmic: true, precise: true) }"
println "out_degree: ${ project.vertexAttributes['out_degree'].histogram(logarithmic: true, precise: true) }"

