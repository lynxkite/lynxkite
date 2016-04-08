// Loads edges.csv from the test set into the 'test_edges' table.


testDataSet = params.testDataSet ?: 'fake_westeros_100k'
edgePath = lynx.resolvePath(
  'S3$/lynxkite-test-data/' + testDataSet + '/edges.csv')

edgeDF = lynx.sqlContext.read()
  .format('com.databricks.spark.csv')
  .option('header', 'true')
  .load(edgePath)
lynx.saveAsTable(edgeDF, 'test_edges')

