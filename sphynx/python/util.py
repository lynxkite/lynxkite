'''Simple access to operation parameters, input, and outputs.'''
import json
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys


class Op:
  def __init__(self, argv=None):
    if argv is None:
      argv = sys.argv
    self.datadir = sys.argv[1]
    op = json.loads(sys.argv[2])
    self.params = op['Operation']['Data']
    self.inputs = op['Inputs']
    self.outputs = op['Outputs']

  def input_parquet(self, name):
    '''Returns the input as a PyArrow ParquetFile. Useful if you only need the metadata.'''
    return pq.ParquetFile(f'{self.datadir}/{self.inputs[name]}/data.parquet')

  def input_table(self, name):
    '''Reads the input as a PyArrow Table. Useful if you only need some of the data.'''
    return pq.read_table(f'{self.datadir}/{self.inputs[name]}/data.parquet')

  def input(self, name):
    '''Reads the input as a Pandas DataFrame. Useful if you need all the data.'''
    return pd.read_parquet(f'{self.datadir}/{self.inputs[name]}/data.parquet')

  def output(self, name, values, defined=None):
    '''Writes a list or Numpy array to disk.'''
    if defined is None:
      defined = [True] * len(values)
    result = pd.DataFrame({'value': values, 'defined': defined})
    path = self.datadir + '/' + self.outputs[name]
    os.makedirs(path, exist_ok=True)
    t = pa.Table.from_pandas(result)
    # We must set nullable=False or Go cannot read it.
    schema = pa.schema([
        pa.field('value', pa.float64(), nullable=False),
        pa.field('defined', pa.bool_(), nullable=False)])
    t = pa.Table.from_arrays(t.columns, schema=schema)
    pq.write_table(t, path + '/data.parquet')
    with open(path + '/type_name', 'w') as f:
      f.write('DoubleAttribute')  # TODO: More types.
    with open(path + '/_SUCCESS', 'w'):
      pass
