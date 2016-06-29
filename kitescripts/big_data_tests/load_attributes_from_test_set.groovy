// Loads vertex_attributes.csv from the test set into
// 'test_vertex_attributes' table.

testDataSet = params.testDataSet

vertexAttributePath = lynx.resolvePath(
  'S3$/lynxkite-test-data/' + testDataSet + '/vertex_attributes.csv')
vertexAttributeDF = lynx.sqlContext.read()
  .format('com.databricks.spark.csv')
  .option('header', 'true')
  .load(vertexAttributePath)
lynx.saveAsTable(vertexAttributeDF, 'test_vertex_attributes')

