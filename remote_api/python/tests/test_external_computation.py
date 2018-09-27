import lynx.kite
import numpy as np
import os
import pandas as pd
import unittest


class TestExternalComputation(unittest.TestCase):

  def test_external_computation(self):
    lk = lynx.kite.LynxKite()

    def title_names(inpath, outpath):
      df = pd.read_parquet(inpath.replace('file:', ''))
      df['titled_name'] = np.where(df.gender == 'Female', 'Ms ' + df.name, 'Mr ' + df.name)
      df = df[['titled_name', 'gender']]
      op = outpath.replace('file:', '')
      os.makedirs(op, exist_ok=True)
      df.to_parquet(op + '/part-00000.gzip.parquet', engine='fastparquet', compression='GZIP')
      with open(op + '/_SUCCESS', 'w'):
        pass

    @lynx.kite.subworkspace
    def titled(data, sec=lynx.kite.SideEffectCollector.AUTO):
      return data.apply(title_names, sec)

    eg = lk.createExampleGraph().sql('select name, gender from vertices')
    t = titled(eg)
    t.trigger()
    self.assertTrue(t.df().equals(pd.DataFrame([
        ['Mr Adam', 'Male'],
        ['Ms Eve', 'Female'],
        ['Mr Bob', 'Male'],
        ['Mr Isolated Joe', 'Male'],
    ], columns=['titled_name', 'gender'])))
