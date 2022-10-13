'''This example starts LynxKite with a Spark session we already have in Python.'''
import unittest
import lynx.kite
from pyspark.sql import SparkSession
import os.path
import pandas as pd

jar = os.path.dirname(__file__) + '/../../../target/scala-2.12/lynxkite-0.1-SNAPSHOT.jar'
assert os.path.exists(jar), f'Could not find {jar}'


class TestStartStop(unittest.TestCase):

  def test_can_run_lynxkite_from_python(self):
    # Create a Spark session with the LynxKite jar.
    spark = SparkSession.builder.config('spark.jars', 'file:' + jar).getOrCreate()
    # Pass this Spark session to LynxKite.
    lk = lynx.kite.LynxKite(spark=spark)
    # Run some LynxKite operations.
    df = lk.createExampleGraph().sql(
        'select src_name, edge_weight, dst_name from edges order by edge_weight').df()
    pd.testing.assert_frame_equal(df, pd.DataFrame({
        'src_name': 'Adam Eve Bob Bob'.split(),
        'edge_weight': [1, 2, 3, 4],
        'dst_name': 'Eve Adam Adam Eve'.split()}))
