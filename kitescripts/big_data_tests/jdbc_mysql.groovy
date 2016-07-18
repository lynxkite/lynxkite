// Writes data through JDBC and then reads it back.

/// REQUIRE_SCRIPT load_edges_from_test_set.groovy

project = lynx.newProject()
project.importVertices(
  'id-attr': 'id',
  table: lynx.openTable('test_edges'))
before = project.scalars['vertex_count'].toDouble()

start_time = System.currentTimeMillis()

db = System.getenv('MYSQL')
println "writing to jdbc:mysql://$db:3306/db?user=root&password=rootroot"
p = new java.util.Properties()
p.setProperty("driver", "com.mysql.jdbc.Driver")
project.vertexDF.write().mode('overwrite').jdbc(
  "jdbc:mysql://$db:3306/db?user=root&password=rootroot",
  'test_edges',
  p)

write_done = System.currentTimeMillis()
println "JDBC write: ${ (write_done - start_time) / 1000 } seconds"

partitions = project.vertexDF.rdd().partitions().length
df = lynx.sqlContext.read().jdbc(
  "jdbc:mysql://$db:3306/db?user=root&password=rootroot",
  'test_edges',
  'id',
  java.lang.Long.MIN_VALUE,
  java.lang.Long.MAX_VALUE,
  partitions,
  p)
project.importVertices(
  'id-attr': 'internal-id',
  table: lynx.saveAsTable(df, 't'))
after = project.scalars['vertex_count'].toDouble()

read_done = System.currentTimeMillis()
println "JDBC read: ${ (read_done - write_done) / 1000 } seconds"
println "vertices before: $before after: $after"
