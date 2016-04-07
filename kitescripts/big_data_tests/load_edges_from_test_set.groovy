// Loads edges.csv from the test set into the 'test_edges' table.


testSet = params.testSet ?: 'fake_westeros_100k'
edgePath = lynx.resolvePath('S3$/lynxkite-test-data/' + testSet + '/edges.csv')

edgeDF = lynx.sqlContext.read()
  .format('com.databricks.spark.csv')
  .option('header', 'true')
  .load(edgePath)
lynx.saveAsTable(edgeDF, 'test_edges')

