// A benchmark script for importing multiple vertex attributes.
project = lynx.newProject()
size = 10000000
project.newVertexSet(size: size)
project.addGaussianVertexAttribute(name: 'random1', seed: 1)
project.addGaussianVertexAttribute(name: 'random2', seed: 2)
project.addGaussianVertexAttribute(name: 'random3', seed: 3)

df = project.sql('select random1, random2, random3 from vertices')
df.write().format('com.databricks.spark.csv')
  .option('header', 'true').mode('overwrite').save('importtest')

println "exported $size vertices"

project = lynx.newProject()
df = lynx.sqlContext.read()
  .format('com.databricks.spark.csv').option('header', 'true').load('importtest')
project.importVertices(
  table: lynx.saveTable(df, 'importtest'),
  'id-attr': 'id')

start_time = System.currentTimeMillis()
println "random1: ${ project.vertexAttributes['random1'].histogram() }"
println "random2: ${ project.vertexAttributes['random2'].histogram() }"
println "random3: ${ project.vertexAttributes['random3'].histogram() }"
println "time: ${ (System.currentTimeMillis() - start_time) / 1000 } seconds"

System.console().readLine '...'
