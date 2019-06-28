import lynx.kite
import numpy as np
import os
import pandas as pd
import unittest


class TestExternalComputation(unittest.TestCase):

  def test_pandas(self):
    lk = lynx.kite.LynxKite()

    @lynx.kite.external
    def title_names(table):
      df = table.pandas()
      df['titled_name'] = np.where(df.gender == 'Female', 'Ms ' + df.name, 'Mr ' + df.name)
      return df

    @lynx.kite.external
    def title_names_with_download(table):
      df = table._pandas_via_lk_download()
      df['titled_name'] = np.where(df.gender == 'Female', 'Ms ' + df.name, 'Mr ' + df.name)
      return df

    eg = lk.createExampleGraph().sql('select name, gender from vertices')

    t1 = title_names(eg)
    t2 = title_names_with_download(eg)

    for t in [t1, t2]:
      t.trigger()
      self.assertTrue(t.sql('select titled_name from input').df().equals(
          pd.DataFrame({'titled_name': [
              'Mr Adam',
              'Ms Eve',
              'Mr Bob',
              'Mr Isolated Joe',
          ]})))

  def test_pyspark(self):
    lk = lynx.kite.LynxKite()
    try:
      from pyspark.sql import SparkSession
    except ImportError:
      print('Skipping PySpark test. "pip install pyspark" if you want to run it.')
      return
    spark = SparkSession.builder.appName('test').getOrCreate()

    @lynx.kite.external
    def stats(table):
      df = table.spark(spark)
      return df.groupBy('gender').count()

    eg = lk.createExampleGraph().sql('select * from vertices')
    t = stats(eg)
    t.trigger()
    self.assertTrue(t.sql('select * from input').df().equals(
        pd.DataFrame({'gender': ['Female', 'Male'], 'count': [1.0, 3.0]})))

  def test_filename(self):
    lk = lynx.kite.LynxKite()

    @lynx.kite.external
    def title_names(table):
      df = table.pandas()
      df['titled_name'] = np.where(df.gender == 'Female', 'Ms ' + df.name, 'Mr ' + df.name)
      path_lk = 'DATA$/test/external-computation'
      path = lk.get_prefixed_path(path_lk).resolved.replace('file:', '')
      os.makedirs(path, exist_ok=True)
      path = path + '/part-0'
      df.to_parquet(path)
      return path_lk

    eg = lk.createExampleGraph().sql('select name, gender from vertices')
    t = title_names(eg)
    t.trigger()
    self.assertTrue(t.sql('select titled_name from input').df().equals(
        pd.DataFrame({'titled_name': [
            'Mr Adam',
            'Ms Eve',
            'Mr Bob',
            'Mr Isolated Joe',
        ]})))

  def test_multiple_inputs(self):
    lk = lynx.kite.LynxKite()

    @lynx.kite.external
    def join(t1, t2):
      return t1.pandas().join(t2.pandas())

    eg = lk.createExampleGraph()
    t = join(eg.sql('select age from vertices'), eg.sql('select name from vertices'))
    t.trigger()
    self.assertTrue(t.sql('select age, name from input').df().equals(
        pd.DataFrame({
            'age': [20.3, 18.2, 50.3, 2.0], 'name': ['Adam', 'Eve', 'Bob', 'Isolated Joe']})))

  def test_ordering(self):
    lk = lynx.kite.LynxKite()

    @lynx.kite.external
    def join(t1, t2):
      return t1.pandas().join(t2.pandas())

    @lynx.kite.subworkspace
    def compute(eg, sec=lynx.kite.SideEffectCollector.AUTO):
      ages = eg.sql('select age from vertices')
      names = eg.sql('select names from vertices')
      ages.saveToSnapshot(path='ages').register(sec)
      t = join(ages, names).register(sec)
      eg.saveToSnapshot(path='names').register(sec)
      return t

    sec = lynx.kite.SideEffectCollector()
    compute(lk.createExampleGraph()).register(sec)
    # The triggerables are in insertion order.
    self.assertEqual([
        'saveToSnapshot', 'exportToParquet', 'exportToParquet', 'externalComputation2',
        'saveToSnapshot'],
        [t.base.operation for t in sec.all_triggerables()])
    deps = lynx.kite.BoxPath.dependencies(list(sec.all_triggerables()))
    # Dependencies are correctly found.
    self.assertEqual({
        'compute_?/join_0/exportToParquet_0': {'compute_?/saveToSnapshot_1'},
        'compute_?/join_0/exportToParquet_1': {'compute_?/saveToSnapshot_1'},
        'compute_?/join_0/externalComputation2_0': {'compute_?/join_0/exportToParquet_0',
                                                    'compute_?/join_0/exportToParquet_1',
                                                    'compute_?/saveToSnapshot_0',
                                                    'compute_?/saveToSnapshot_1'},
        'compute_?/saveToSnapshot_0': {'compute_?/saveToSnapshot_1'},
        'compute_?/saveToSnapshot_1': set(),
    }, {str(k): set(str(v) for v in vs) for (k, vs) in deps.items()})
    # Serialized trigger order puts exporting before the external computation.
    self.assertEqual([
        'compute_?/saveToSnapshot_1',
        'compute_?/saveToSnapshot_0',
        'compute_?/join_0/exportToParquet_0',
        'compute_?/join_0/exportToParquet_1',
        'compute_?/join_0/externalComputation2_0',
    ], [str(bp) for bp in lynx.kite.serialize_deps(deps)])

  def test_lk(self):
    lk = lynx.kite.LynxKite()

    @lynx.kite.external
    def title_names(table):
      return table.lk().sql('''
          select concat(case when gender = "Female" then "Ms " else "Mr " end, name) as titled_name
          from input''')

    eg = lk.createExampleGraph().sql('select name, gender from vertices')
    t = title_names(eg)
    t.trigger()
    self.assertTrue(t.sql('select titled_name from input').df().equals(
        pd.DataFrame({'titled_name': [
            'Mr Adam',
            'Ms Eve',
            'Mr Bob',
            'Mr Isolated Joe',
        ]})))
