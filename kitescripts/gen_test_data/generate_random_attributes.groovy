// Create random attributes for a graph in a CSV files.

// Usage examples:
//   emr.sh batch emr_test_spec .../kitescripts/gen_test_data/generate_random_attributes.groovy --  numVertices:100000 testSetName:fake_westeros_v3_100k_2m
//   emr.sh batch emr_test_spec .../kitescripts/gen_test_data/generate_random_attributes.groovy -- numVertices:5000000 testSetName:fake_westeros_v3_5m_145m
//   emr.sh batch emr_test_spec .../kitescripts/gen_test_data/gen_test_data/generate_random_attributes.groovy -- numVertices:10000000 testSetName:fake_westeros_v3_10m_303m
//   emr.sh batch emr_test_spec .../kitescripts/gen_test_data/generate_random_attributes.groovy -- numVertices:25000000 testSetName:fake_westeros_v3_25m_799m

numVertices = params.numVertices.toInteger()
testSetName = params.testSetName

vertexAttributePath = lynx.resolvePath('S3$/lynxkite-test-data/' + testSetName + '/vertex_attributes.csv')

project = lynx.newProject()
project.newVertexSet(size: numVertices)
project.addRandomVertexAttribute(name: 'attr1', 'dist': 'Standard Uniform', seed: '1001')
project.addRandomVertexAttribute(name: 'attr2', 'dist': 'Standard Uniform', seed: '1002')
vertexDF = project.sql(
  'select ordinal, attr1,attr2 from vertices').toDF('id', 'attr1', 'attr2')
vertexDF.write()
  .format('com.databricks.spark.csv')
  .option('header', 'true')
  .mode('overwrite')
  .save(vertexAttributePath)

println "attribute export complete ${testSetName}"
