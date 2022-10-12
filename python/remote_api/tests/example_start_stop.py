'''This example starts LynxKite with a Spark session we already have in Python.'''
import lynx.kite
from pyspark.sql import SparkSession
spark = lynx.kite.add_jar(SparkSession.builder).getOrCreate()
lk = lynx.kite.LynxKite(spark=spark)
print(lk.createExampleGraph().sql('select * from edges').df())
