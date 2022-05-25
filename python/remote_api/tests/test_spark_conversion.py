import unittest
import lynx.kite
from pyspark.sql import Row, SparkSession

lk = lynx.kite.LynxKite()
spark = SparkSession.builder.getOrCreate()


class TestSparkConversion(unittest.TestCase):

  def test_spark_to_lynxkite(self):
    people_df = spark.createDataFrame([
      Row(name='Adam', age=23),
      Row(name='Eve', age=34),
      Row(name='Bob', age=45),
    ])
    relationships_df = spark.createDataFrame([
      Row(a='Adam', b='Eve', weight=3),
      Row(a='Bob', b='Eve', weight=4),
      Row(a='Eve', b='Adam', weight=5),
    ])
    g = lk.from_spark(people_df).useTableAsVertices()
    g = lk.useTableAsEdges(
      g,
      lk.from_spark(relationships_df),
      attr='name', src='a', dst='b')
    self.assertEqual(3, g.sql('select count(*) from vertices').df().values[0][0])

  def test_lynxkite_to_spark(self):
    t = lk.createExampleGraph().sql('select name, age from vertices')
    df = t.spark(spark)
    self.assertEqual(
      ['Isolated Joe', 'Eve', 'Adam', 'Bob'],
      [row[0] for row in sorted(df.collect(), key=lambda row: row[1])])
