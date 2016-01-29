// Simple JavaScript benchmark.
start_time = System.currentTimeMillis()

vertices = 10000000
project = lynx.newProject()
project.newVertexSet(size: vertices)
project.vertexAttributeToDouble(attr: 'ordinal')
project.derivedVertexAttribute(
  output: 'x',
  type: 'double',
  expr: 'ordinal * ordinal')

println "vertices: $vertices"
println "x: ${ project.vertexAttributes['x'].histogram() }"
println "time: ${ (System.currentTimeMillis() - start_time) / 1000 } seconds"
