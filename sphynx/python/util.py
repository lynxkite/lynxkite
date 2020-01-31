'''Simple access to operation parameters, input, and outputs.'''
import json
import numpy as np
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys

DoubleAttribute = 'DoubleAttribute'
DoubleVectorAttribute = 'DoubleVectorAttribute'
DoubleTuple2Attribute = 'DoubleTuple2Attribute'
PA_TYPES = {
    DoubleAttribute: pa.float64(),
    DoubleVectorAttribute: pa.list_(pa.float64()),
    DoubleTuple2Attribute: pa.struct([('x', pa.float64()), ('y', pa.float64())]),
}


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

  def input(self, name, type=None):
    '''Reads the input as a Pandas DataFrame. Useful if you need all the data.'''
    df = pd.read_parquet(f'{self.datadir}/{self.inputs[name]}/data.parquet')
    if list(df.columns) == ['value', 'defined']:
      vs = df.value.values
      vs[~df.defined] = np.nan
      if type == 'DoubleVectorAttribute':
        vs = np.array(list(list(v) for v in vs))
      return vs
    return df

  def output(self, name, values, *, type, defined=None):
    '''Writes a list or Numpy array to disk.'''
    if hasattr(values, 'numpy'):
      values = values.numpy()
    if defined is None:
      if type == pa.float64():
        defined = list(~np.isnan(values))
      else:
        defined = [v is not None for v in values]
    if not isinstance(values, list):
      values = list(values)
    if type == DoubleTuple2Attribute:
      values = [{'x': v[0], 'y': v[1]} for v in values]
    patype = PA_TYPES[type]
    values = pa.array(values, patype)
    defined = pa.array(defined, pa.bool_())
    path = self.datadir + '/' + self.outputs[name]
    os.makedirs(path, exist_ok=True)
    # We must set nullable=False or Go cannot read it.
    schema = pa.schema([
        pa.field('value', patype, nullable=False),
        pa.field('defined', pa.bool_(), nullable=False)])
    t = pa.Table.from_arrays([values, defined], schema=schema)
    pq.write_table(t, path + '/data.parquet')
    with open(path + '/type_name', 'w') as f:
      f.write(type)
    with open(path + '/_SUCCESS', 'w'):
      pass
