// This script generates a graph that has a similar degree distribution like the
// one that caused import trouble at Westeros. (Number of vertices goes down
// exponentially as degree goes up exponentially.)
//
// Usage examples:
//   tools/emr_based_test.sh backend gen_test_data/generate_fake_westeros \
//     testDataSet:fake_westeros_xt_100k numVertices:100000 numBatches:100 maxDegree:1000
//
//   tools/emr_based_test.sh backend gen_test_data/generate_fake_westeros \
//     testDataSet:fake_westeros_xt_25m numVertices:25000000 numBatches:20 maxDegree:1250000

testSet = params.testDataSet ?: 'tmp_test_data'

numVertices = (params.numVertices ?: '100000').toInteger()
numBatches = (params.numBatches ?: '100').toInteger()
maxDegree = (params.maxDegree ?: '1000').toInteger()
// Note: reverse edges are added automatically.

// Adds a vertex which is connected to every other vertex.
addGlobalHubVertex = (params.addGlobalHubVertex ?: 'true').toBoolean()

assert (maxDegree <= numVertices / numBatches)
ratio = Math.exp(Math.log(maxDegree) / (numBatches - 1))


edgeExportPath = lynx.resolvePath('S3$/lynxkite-test-data/' + testSet + '/edges.csv')
vertexExportPath = lynx.resolvePath('S3$/lynxkite-test-data/' + testSet + '/vertices.csv')

project = lynx.newProject()

numEffectiveVertices = numVertices
if (addGlobalHubVertex) {
  numEffectiveVertices *= 2
}
project.newVertexSet(size: numEffectiveVertices)
project.vertexAttributeToDouble(attr: 'ordinal')
project.renameVertexAttribute(from: 'ordinal', to: 'src')
// We try to generate a simple graph which has similar degree distribution
// to the "westeros" case. The goal is that we want to see exponentially
// decreasing vertex counts as degree goes up in the logarithmic degree histogram.
// To achieve that, we will created the edges in batches of fixed size, but the
// amount of dst-side vertices per batches will be halved* after each iteration.
// *in case of ratio = 2.0 (in the general case, they are divided by ratio)
// So we start
// with creating N vertices of degree 1, and then
// N/2 vertices of degree 2,
// N/4 vertices of degree 4,
// and so on. Notes:
// - This algorithm only controls the in-degrees. Some vertices will get an out-degree of 1
// and the rest will get an out-degree of 0.
// - Each vertex of the current graph will generate one edge in the resulting
// CSV file.
project.derivedVertexAttribute(
  expr:
    'var numVertices = ' + numVertices + ';\n' +
    'var ratio = ' + ratio + ';\n' +
    'var numBatches = ' + numBatches + ';\n' +
    'var batchSize = Math.floor(numVertices / numBatches);\n' +
    'var result = numVertices;\n' +  // extra vertex to become the global hub
    'if (src < numVertices) {\n' +
    '  var continuousPos = src / batchSize;\n' +
    '  var batchId = Math.floor(continuousPos);\n' +
    '  var frac = continuousPos - batchId;\n' +
    // (batchId + 1) % numBatches
    '  var shiftedBatchId = batchId + 1 >= numBatches - 0.001 ? 0 : batchId + 1;\n' +
    // Each batch uses vertices from the range
    // shiftedBatchId * batchSize -- (shiftedBatchId+1) * batchSize
    '  var startingDstSideVertex = shiftedBatchId * batchSize;\n' +
    '  var fpFloorFix = 1.0 / batchSize;\n' + // Offset to fix rounding.
                                            // Makes sense when testing with ratio = 2.0
                                            // Not really verified.
    '  var thisVertexOffset = Math.floor(frac * batchSize / Math.pow(ratio, batchId) + fpFloorFix);\n' +
    '  result = startingDstSideVertex + thisVertexOffset;\n' +
    '}\n' +
    'result.toFixed();',
  output: 'dst',
  type: 'string')
if (addGlobalHubVertex) {
  project.derivedVertexAttribute(
    expr: 'src % ' + numVertices,
    output: 'src',
    type: 'double')
}

// Convert src to string:
project.derivedVertexAttribute(expr: 'src', output: 'src', type: 'double')
project.derivedVertexAttribute(
  expr: 'src.toFixed()',
  output: 'src',
  type: 'string')

// Add reversed edges:
edgeDF = project.sql(
  'select src,dst from vertices')
revEdgeDF = edgeDF.select(edgeDF.col("dst"), edgeDF.col("src")).toDF("src", "dst")
edgeDF = edgeDF.unionAll(revEdgeDF)

edgeDF.write()
  .format('com.databricks.spark.csv')
  .option('header', 'true')
  .mode('overwrite')
  .save(edgeExportPath)

// Obtain vertex ids (assumes that edges are symmetric):
vertexDF = edgeDF.select(edgeDF.col("src")).distinct().toDF("vertex_id")
vertexDF.write()
  .format('com.databricks.spark.csv')
  .option('header', 'true')
  .mode('overwrite')
  .save(vertexExportPath)

println "exported ${numEffectiveVertices * 2} edges"
