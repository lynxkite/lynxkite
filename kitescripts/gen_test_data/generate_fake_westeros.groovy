// This script generates a graph that has a somewhat similar degree distribution like the
// one that caused import trouble at Westeros. (Number of vertices goes down
// exponentially as degree goes up exponentially.)
//
// Usage examples:
//   ./stage/tools/emr.sh batch ./stage/tools/emr_spec_template ./kitescripts/gen_test_data/generate_fake_westeros.groovy --  numVertices:100000 maxDegree:100000 dropoff:1.1
//   ./stage/tools/emr.sh batch ./stage/tools/emr_spec_template ./kitescripts/gen_test_data/generate_fake_westeros.groovy -- numVertices:5000000 maxDegree:5000000 dropoff:1.1
//   ./stage/tools/emr.sh batch ./stage/tools/emr_spec_template ./kitescripts/gen_test_data/gen_test_data/generate_fake_westeros.groovy -- numVertices:10000000 maxDegree:10000000 dropoff:1.1
//   ./stage/tools/emr.sh batch ./stage/tools/emr_spec_template ./kitescripts/gen_test_data/generate_fake_westeros.groovy -- numVertices:25000000 maxDegree:25000000 dropoff:1.1

// See issues/3690 before running this.

numVertices = (params.numVertices ?: '100').toInteger()
maxDegree = (params.maxDegree ?: '4').toInteger()
dropoff = (params.dropoff ?: '1.1').toDouble()

// Compute stats for debugging and file-name generation:
numEdges = 0;
for (i = 1; i < numVertices; ++i) {
  numEdges += 2 * ((int) (maxDegree / Math.max(1.0, i * dropoff)).round())
}
println "numVertices= $numVertices, numEdges= $numEdges"

def humanizeNumber(num) {
  s = ['', 'k', 'm', 'g', 't']
  id = 0
  while (Math.round(num) >= 1000.0) {
    id++
    num = num / 1000.0
  }
  return (((int)Math.round(num)).toString()) + s[id]
}

testSetName = "fake_westeros_v3_${humanizeNumber(numVertices)}_${humanizeNumber(numEdges)}"
println "test set name: ${testSetName}"

edgeExportPath = lynx.resolvePath('S3$/lynxkite-test-data/' + testSetName + '/edges.csv')
vertexExportPath = lynx.resolvePath('S3$/lynxkite-test-data/' + testSetName + '/vertices.csv')

project = lynx.newProject()

// The vertices here correspond to vertices of the generated graph.
project.newVertexSet(size: numVertices)

// Export vertices.
vertexDF = project.sql('select ordinal from vertices').toDF("vertex_id")
vertexDF.write()
  .format('com.databricks.spark.csv')
  .option('header', 'true')
  .mode('overwrite')
  .save(vertexExportPath)

project.vertexAttributeToDouble(attr: 'ordinal')
project.renameVertexAttribute(from: 'ordinal', to: 'src')

project.derivedVertexAttribute(
  expr:
    'var x = 0;\n' +
    'if (src > 0) x = ' + maxDegree + ' / Math.max(1.0, src * ' + dropoff + ');\n' +
    'x;',
  output: 'neededDegree',
  type: 'double')

// The vertices after this correspond to the edges of the generated graph.
project.splitVertices(rep: 'neededDegree', idattr: 'id', idx: 'idx')
project.addRandomVertexAttribute(name: 'dstRandom', 'dist': 'Standard Uniform', seed: '420')
project.derivedVertexAttribute(
  expr: '(dstRandom * (' + numVertices + ' + 1)).toFixed()',
  output: 'dst',
  type: 'string')
project.derivedVertexAttribute(
  expr: 'src.toFixed()',
  output: 'src',
  type: 'string')

// Obtain "normal" generated edges and merge them with "hub" edges created earlier.
forwardEdgeDF = project.
  sql('select src,dst from vertices')

// Create reversed edges:
edgeDF = forwardEdgeDF.unionAll(forwardEdgeDF.select("dst", "src"))

edgeDF.write()
  .format('com.databricks.spark.csv')
  .option('header', 'true')
  .mode('overwrite')
  .save(edgeExportPath)

println "export complete ${testSetName}"
