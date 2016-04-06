// Tests the "Import vertices and edges from a single table" FE operation.
// The edges are loaded from edges.csv of the testSet into a table "test_edges"

testSet = params.testSet ?: 'fake_westeros_100k'
importPath = lynx.resolvePath('S3$/lynxkite-test-data/' + testSet + '/edges.csv')

project = lynx.newProject()
df = lynx.sqlContext.read()
  .format('com.databricks.spark.csv')
  .option('header', 'true')
  .load(importPath)
project.importVerticesAndEdgesFromASingleTable(
  table: lynx.saveAsTable(df, 'test_edges'),
  src: 'src',
  dst: 'dst')

project.degree(direction: 'incoming edges', name: 'in_degree')
project.degree(direction: 'outgoing edges', name: 'out_degree')

println "in_degree: ${ project.vertexAttributes['in_degree'].histogram(logarithmic: true, precise: true) }"
println "out_degree: ${ project.vertexAttributes['out_degree'].histogram(logarithmic: true, precise: true) }"


