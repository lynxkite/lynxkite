// Parameters

def getParameter(paramName, defaultValue) {
    if (params.containsKey(paramName))
       return params[paramName]
    else
       return defaultValue
}

seed = getParameter('seed', '31415')
seed2 = getParameter('seed2', (seed.toInteger() + 42).toString())
vertices = getParameter('vertices', '30')
ebSize = getParameter('ebSize', '5')
mostCallsPossible = getParameter('mostCalls', '3')
output = getParameter('output', 'fprandom')

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

// Give each vertex a unique id between 0 and vertices - 1
project.addRandomVertexAttribute(
  name: 'random',
  dist: 'Standard Uniform',
  seed: seed2
)
project.addRankAttribute(
  keyattr: 'random',
  order: 'ascending',
  rankattr: 'originalUniqueId'
)



project.exportVertexAttributesToFile(
  path: 'DATA$exports/' + output + '_vertices',
  link: 'vertices_csv',
  attrs: 'originalUniqueId',
  format: 'CSV'
)

project.exportEdgeAttributesToFile(
  path: 'DATA$exports/' + output + '_edges',
  link: 'edges_csv',
  attrs: 'originalCalls',
  id_attr: 'originalUniqueId',
  format: 'CSV'
)
