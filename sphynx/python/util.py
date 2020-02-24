'''Simple access to operation parameters, input, and outputs.'''
import json
import numpy as np
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import json
import torch


DoubleAttribute = 'DoubleAttribute'
StringAttribute = 'StringAttribute'
DoubleVectorAttribute = 'DoubleVectorAttribute'
DoubleTuple2Attribute = 'DoubleTuple2Attribute'
PA_TYPES = {
    DoubleAttribute: pa.float64(),
    StringAttribute: pa.string(),
    DoubleVectorAttribute: pa.list_(pa.field('element', pa.float64(), nullable=False)),
    DoubleTuple2Attribute: pa.list_(pa.field('element', pa.float64(), nullable=False)),
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

  def input_model(self, name):
    '''Loads a Pytorch model.'''
    path = f'{self.datadir}/{self.inputs[name]}/model.pt'
    return torch.load(path)

  def input_scalar(self, name, value):
    '''Reads a scalar from disk.'''
    with open(f'{self.datadir}/{self.inputs[name]}/serialized_data') as f:
      return json.load(f)

  def output(self, name, values, *, type, defined=None):
    '''Writes a list or Numpy array to disk.'''
    if hasattr(values, 'numpy'):  # Turn PyTorch Tensors into Numpy arrays.
      values = values.numpy()
    if defined is None:
      if type == pa.float64():
        defined = list(~np.isnan(values))
      else:
        defined = [v is not None for v in values]
    if not isinstance(values, list):
      values = list(values)
    self.write_columns(name, type, {
        'value': pa.array(values, PA_TYPES[type]),
        'defined': pa.array(defined, pa.bool_()),
    })

  def write_type(self, path, type):
    print('writing', type, 'to', path)
    os.makedirs(path, exist_ok=True)
    with open(path + '/type_name', 'w') as f:
      f.write(type)

  def write_columns(self, name, type, columns):
    path = self.datadir + '/' + self.outputs[name]
    self.write_type(path, type)
    # We must set nullable=False or Go cannot read it.
    schema = pa.schema([
        pa.field(name, a.type, nullable=False) for (name, a) in columns.items()])
    t = pa.Table.from_arrays(list(columns.values()), schema=schema)
    pq.write_table(t, path + '/data.parquet')
    with open(path + '/_SUCCESS', 'w'):
      pass

  def output_vs(self, name, count):
    '''Writes a vertex set to disk. You just specify the vertex count.'''
    self.write_columns(name, 'VertexSet', {'sparkId': pa.array(range(count), pa.int64())})

  def output_es(self, name, edge_index):
    '''Writes an edge bundle specified as a 2xN matrix to disk.'''
    if hasattr(edge_index, 'numpy'):
      edge_index = edge_index.numpy()
    src, dst = edge_index
    self.write_columns(name, 'EdgeBundle', {
        'src': pa.array(src, pa.int64()),
        'dst': pa.array(dst, pa.int64()),
        'sparkId': pa.array(range(len(src)), pa.int64()),
    })
    if name + '-idSet' in self.outputs:
      self.write_columns(name + '-idSet', 'VertexSet', {
          'sparkId': pa.array(range(len(src)), pa.int64()),
      })

  def output_scalar(self, name, value):
    '''Writes a scalar to disk.'''
    path = self.datadir + '/' + self.outputs[name]
    self.write_type(path, 'Scalar')
    with open(path + '/serialized_data', 'w') as f:
      json.dump(value, f)
    with open(path + '/_SUCCESS', 'w'):
      pass

  def output_model(self, name, model, description):
    '''Writes PyTorch model to disk.'''
    path = self.datadir + '/' + self.outputs[name]
    os.makedirs(path, exist_ok=True)
    torch.save(model, path + '/model.pt')
    self.output_scalar(name, description)
