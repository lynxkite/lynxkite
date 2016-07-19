// Writes data through JDBC and then reads it back.

/// REQUIRE_SCRIPT load_edges_from_test_set.groovy

project = lynx.newProject()
project.importVertices(
  'id-attr': 'id',
  table: lynx.openTable('test_edges'))
before = project.scalars['vertex_count'].toDouble()

start_time = System.currentTimeMillis()

db = System.getenv('MYSQL')
url = (
  "jdbc:mysql://$db:3306/db?user=root&password=rootroot" +
  '&rewriteBatchedStatements=true')
println "writing $before records to $url"
p = new java.util.Properties()
p.setProperty('driver', 'com.mysql.jdbc.Driver')
p.setProperty('batchsize', '10000')
project.vertexDF.write().mode('overwrite').jdbc(url, 'test_edges', p)

write_done = System.currentTimeMillis()
println "JDBC write: ${ (write_done - start_time) / 1000 } seconds"

partitions = project.vertexDF.rdd().partitions().length
df = lynx.sqlContext.read().jdbc(
  url,
  'test_edges',
  'id',
  java.lang.Long.MIN_VALUE,
  java.lang.Long.MAX_VALUE,
  partitions,
  p)

// Just iterate over the JDBC results to measure JDBC performance on its own.
after = df.javaRDD().count()
read_done = System.currentTimeMillis()
println "JDBC read: $after records in ${ (read_done - write_done) / 1000 } seconds"

// Import into a project.
project.importVertices(
  'id-attr': 'internal-id',
  table: lynx.saveAsTable(df, 't'))
after = project.scalars['vertex_count'].toDouble()
import_done = System.currentTimeMillis()
println "JDBC import: $after records in ${ (import_done - read_done) / 1000 } seconds"
