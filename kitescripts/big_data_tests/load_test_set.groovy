// Loads test data CSVs into LynxKite tables
// Run this before other tests to create the necessary tables.

// The vertices are loaded from edges.csv of the testSet into a table "test_vertices"
// The edges are loaded from edges.csv of the testSet into a table "test_edges"

// Available test sets:
//  fake_westeros_100k
//  fake_westeros_100m
//  fake_westeros_2g
//  twitter
//  twitter_sampled10p

testSet = params.testSet ?: 'fake_westeros_100k'
edgePath = lynx.resolvePath('S3$/lynxkite-test-data/' + testSet + '/edges.csv')
vertexPath = lynx.resolvePath('S3$/lynxkite-test-data/' + testSet + '/vertices.csv')

vertexDF = lynx.sqlContext.read()
  .format('com.databricks.spark.csv')
  .option('header', 'true')
  .load(vertexPath)
lynx.saveAsTable(vertexDF, 'test_vertices')

edgeDF = lynx.sqlContext.read()
  .format('com.databricks.spark.csv')
  .option('header', 'true')
  .load(edgePath)
lynx.saveAsTable(edgeDF, 'test_edges')

