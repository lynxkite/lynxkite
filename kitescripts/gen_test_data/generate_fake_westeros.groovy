numVertices = params.numVertices ?: '100000'
numBatches = params.numBatches ?: '100'
ratio = params.ratio ?: '1.08'
// The highest degree will be min(ratio^(numBatches - 1), numVertices / numBatches)
// Parameters used to generate test data:
// 100,000       / 100 / 1.08 --> fake_westeros_100k
// 100,000,000   / 100 / 1.15 --> fake_westeros_100m
// 2,000,000,000 /  20 / 2.65 --> fake_westeros_2g

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
// amount of dst-side vertices per batches will be halved after each iteration.
// So we start
// with creating N vertices of degree 1, and then
// N/2 vertices of degree 2,
// N/4 vertices of degree 4,
// and so on. Notes:
// - A lot of vertices will get a +1 degree compared to that if they show up at
// the src side of an edge.
// - Each vertex of the current graph will generate one edge in the resulting
// CSV file.
// - The exact ratio between batches is not 2.0 but 1.3
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
