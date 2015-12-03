// Create a random 'call graph' suitable as input for
// fingerprint_split_test.groovy.

seed = params.containsKey('seed') ? params['seed'] : '31415'
vertices = params.containsKey('vertices') ? params['vertices'] : '30'
ebSize = params.containsKey('ebSize') ? params['ebSize'] : '5'
mostCallsPossible = params.containsKey('mostCalls') ? params['mostCalls'] : '3'
output = params.containsKey('output') ? params['output'] : 'fprandom'
project=lynx.newProject('random input graph for fp')

project.newVertexSet(size: vertices)
project.createRandomEdgeBundle(degree: ebSize, seed: seed)
project.discardLoopEdges()
project.mergeParallelEdges()

// Attach a random weight (number of calls) to each edge.
project.addRandomEdgeAttribute(
  name: 'originalCallsUnif',
  dist: 'Standard Uniform',
  seed: seed
)
project.derivedEdgeAttribute(
  output: 'originalCalls',
  type: 'double',
  expr: 'Math.floor(originalCallsUnif * ' + mostCallsPossible + ');'
)

// Create a peripheral attribute. In this case, no real peripheral
// attributes exist, so we use a constant 0 almost everywhere.
// The only exception is id 0; we can use this to check
// if the peripheral property is treated correctly in fingerprint_split_test.groovy
project.derivedVertexAttribute(
  output: 'peripheral',
  expr: 'ordinal == 0 ? 1.0 : 0.0',
  type: 'double'
)

project.exportVertexAttributesToFile(
  path: 'DATA$exports/' + output + '_vertices',
  link: 'vertices_csv',
  attrs: 'id,peripheral',
  format: 'CSV'
)

project.exportEdgeAttributesToFile(
  path: 'DATA$exports/' + output + '_edges',
  link: 'edges_csv',
  attrs: 'originalCalls',
  id_attr: 'id',
  format: 'CSV'
)
