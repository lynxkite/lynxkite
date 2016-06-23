// Loads vertices.csv from the test set into the 'test_vertices' table.

testDataSet = params.testDataSet

vertexPath = lynx.resolvePath(
  'S3$/lynxkite-test-data/' + testDataSet + '/vertices.csv')

vertexDF = lynx.sqlContext.read()
  .format('com.databricks.spark.csv')
  .option('header', 'true')
  .load(vertexPath)
lynx.saveAsTable(vertexDF, 'test_vertices')
