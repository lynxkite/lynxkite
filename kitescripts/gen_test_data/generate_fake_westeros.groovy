// This script generates a graph that has a similar degree distribution like the
// one that caused import trouble at Westeros. (Number of vertices goes down
// exponentially as degree goes up exponentially.)

numVertices = (params.numVertices ?: '100000').toInteger()
numBatches = (params.numBatches ?: '100').toInteger()
maxDegree = (params.maxDegree ?: '1000').toInteger()
// Parameters used to generate test data:
// 100,000       / 100 /       1,000 --> fake_westeros_100k
// 100,000,000   / 100 /   1,000,000 --> fake_westeros_100m
// 2,000,000,000 /  20 / 100,000,000 --> fake_westeros_2g
assert (maxDegree <= numVertices / numBatches)
ratio = Math.exp(Math.log(maxDegree) / (numBatches - 1))

testSet = params.testSet ?: 'fake_westeros_100k'

edgeExportPath = lynx.resolvePath('S3$/lynxkite-test-data/' + testSet + '/edges.csv')
vertexExportPath = lynx.resolvePath('S3$/lynxkite-test-data/' + testSet + '/vertices.csv')

project = lynx.newProject()

project.newVertexSet(size: numVertices)
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
    'var numBatches = ' + numBatches + '\n' +
    'var batchSize = Math.floor(numVertices / numBatches);\n' +
    'var continuousPos = src / batchSize;\n' +
    'var batchId = Math.floor(continuousPos);\n' +
    'var frac = continuousPos - batchId;\n' +
    // (batchId + 1) % numBatches
    'var shiftedBatchId = batchId + 1 >= numBatches - 0.001 ? 0 : batchId + 1;\n' +
    // Each batch uses vertices from the range
    // shiftedBatchId * batchSize -- (shiftedBatchId+1) * batchSize
    'var startingDstSideVertex = shiftedBatchId * batchSize;\n' +
    'var fpFloorFix = 1.0 / batchSize;\n' + // Offset to fix rounding.
                                            // Makes sense when testing with ratio = 2.0
    'var thisVertexOffset = Math.floor(frac * batchSize / Math.pow(ratio, batchId) + fpFloorFix);\n' +
    'startingDstSideVertex + thisVertexOffset;\n',
  output: 'dst',
  type: 'double')
project.derivedVertexAttribute(expr: 'src', output: 'src', type: 'double')
project.vertexAttributeToString(attr: 'dst,src')

df = project.sql(
  'select src,dst from vertices')
df.write()
  .format('com.databricks.spark.csv')
  .option('header', 'true')
  .mode('overwrite')
  .save(edgeExportPath)

df = project.sql(
  'select src as vertex_id from vertices')
df.write()
  .format('com.databricks.spark.csv')
  .option('header', 'true')
  .mode('overwrite')
  .save(vertexExportPath)

println "exported $numVertices edges"
