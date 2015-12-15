// Demonstration for using the DataFrames API on project RDDs.
p = lynx.newProject('df-test')
p.exampleGraph()
p.df.show()
p.df.groupBy('gender').count().show()
p.df.registerTempTable('example')
df = lynx.sql('select * from example where gender = "Male"')
df.show()
df.printSchema()
