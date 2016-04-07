// Loads vertices.csv from the test set into the 'test_vertices' table.

testSet = params.testSet ?: 'fake_westeros_100k'
vertexPath = lynx.resolvePath('S3$/lynxkite-test-data/' + testSet + '/vertices.csv')

vertexDF = lynx.sqlContext.read()
  .format('com.databricks.spark.csv')
  .option('header', 'true')
  .load(vertexPath)
lynx.saveAsTable(vertexDF, 'test_vertices')
