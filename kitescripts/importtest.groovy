// A benchmark script for importing multiple vertex attributes.
project = lynx.newProject()
size = 10000000
project.newVertexSet(size: size)
project.addGaussianVertexAttribute(name: 'random1', seed: 1)
project.addGaussianVertexAttribute(name: 'random2', seed: 2)
project.addGaussianVertexAttribute(name: 'random3', seed: 3)
project.exportVertexAttributesToFile(
  path: 'UPLOAD$/importtest',
  link: 'csv',
  attrs: 'random1,random2,random3',
  format: 'CSV')

println "exported $size vertices"

project = lynx.newProject()
project.importVerticesFromCSVFiles(
  files: 'UPLOAD$/importtest/data/part*',
  filter: '',
  header: 'random1,random2,random3',
  omitted: '',
  allow_corrupt_lines: 'no',
  delimiter: ',',
  'id-attr': 'id')

start_time = System.currentTimeMillis()
println "random1: ${ project.vertexAttributes['random1'].histogram() }"
println "random2: ${ project.vertexAttributes['random2'].histogram() }"
println "random3: ${ project.vertexAttributes['random3'].histogram() }"
println "time: ${ (System.currentTimeMillis() - start_time) / 1000 } seconds"

System.console().readLine '...'
