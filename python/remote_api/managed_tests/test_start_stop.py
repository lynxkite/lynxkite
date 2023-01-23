'''This example starts LynxKite with a Spark session we already have in Python.'''
import unittest
import lynx.kite
from pyspark.sql import Row, SparkSession
import os.path
import pandas as pd

jar = os.path.dirname(__file__) + '/../../../target/scala-2.12/lynxkite-0.1-SNAPSHOT.jar'
assert os.path.exists(jar), f'Could not find {jar}'


class TestStartStop(unittest.TestCase):

  def setUp(self):
    global spark, lk
    # Create a Spark session with the LynxKite jar.
    spark = SparkSession.builder.config('spark.jars', 'file:' + jar).getOrCreate()
    # Pass this Spark session to LynxKite.
    lk = lynx.kite.LynxKite(spark=spark)

  def test_can_run_lynxkite_from_python(self):
    # Run some LynxKite operations.
    df = lk.createExampleGraph().sql(
        'select src_name, edge_weight, dst_name from edges order by edge_weight').df()
    pd.testing.assert_frame_equal(df, pd.DataFrame({
        'src_name': 'Adam Eve Bob Bob'.split(),
        'edge_weight': [1, 2, 3, 4],
        'dst_name': 'Eve Adam Adam Eve'.split()}))

  def test_can_pass_dataframe(self):
    df = spark.createDataFrame([
        Row(name='Adam', age=23),
        Row(name='Eve', age=34),
        Row(name='Bob', age=45),
    ])
    res = lk.from_spark(df).sql('select * from input where age < 40').df()
    pd.testing.assert_frame_equal(res, pd.DataFrame({
        'name': ['Adam', 'Eve'],
        'age': [23, 34],
    }))

  def test_can_get_dataframe(self):
    t = lk.createExampleGraph().sql('select name from vertices order by name')
    df = t.spark()
    pd.testing.assert_frame_equal(df.toPandas(), pd.DataFrame({
        'name': ['Adam', 'Bob', 'Eve', 'Isolated Joe'],
    }))
