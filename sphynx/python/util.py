'''Simple access to operation parameters, input, and outputs.'''
import json
import numpy as np
import os
import pandas as pd
import pyarrow as pa
import sys


DoubleAttribute = 'DoubleAttribute'
StringAttribute = 'StringAttribute'
DoubleVectorAttribute = 'DoubleVectorAttribute'
PA_TYPES = {
    DoubleAttribute: pa.float64(),
    StringAttribute: pa.string(),
    DoubleVectorAttribute: pa.list_(pa.field('element', pa.float64(), nullable=False)),
}


class Op:
  def __init__(self, argv=None):
    if argv is None:
      argv = sys.argv
    self.datadir = sys.argv[1]
    op = json.loads(sys.argv[2])
    self.classname = op['Operation']['Class']
    self.params = op['Operation']['Data']
    self.inputs = op['Inputs']
    self.outputs = op['Outputs']

  def input_arrow_table(self, name):
    '''Reads the input as a PyArrow Table.'''
    mmap = pa.memory_map(f'{self.datadir}/{self.inputs[name]}/data.arrow')
    return pa.ipc.open_file(mmap).read_all()

  def input_arrow(self, name):
    '''Reads the input as a PyArrow Array or Table.'''
    table = self.input_arrow_table(name)
    if table.num_columns == 1:
      return table.column(0)
    else:
      return table

  def input_cudf(self, name):
    import cudf
    return cudf.DataFrame.from_arrow(self.input_arrow_table(name))

  def input_vector(self, name):
    '''Reads a DoubleVectorAttribute into a Numpy array.'''
    data = self.input_arrow(name).to_pylist()
    assert None not in data, name + ' has missing values'
    return np.array(data)

  def input(self, name):
    '''Reads the input as a Numpy Array or Pandas DataFrame.'''
    # Makes a copy if the data has nulls or is not a primitive type.
    df = self.input_arrow(name).to_pandas()
    if isinstance(df, pd.Series):
      return df.values
    else:
      return df

  def input_model(self, name):
    '''Loads a Pytorch model.'''
    path = f'{self.datadir}/{self.inputs[name]}/model.pt'
    import torch
    return torch.load(path)

  def input_scalar(self, name):
    '''Reads a scalar from disk.'''
    with open(f'{self.datadir}/{self.inputs[name]}/serialized_data') as f:
      return json.load(f)

  def input_torch_edges(self, name):
    '''Returns an edge bundle input as a PyTorch tensor.'''
    import torch
    es = self.input(name)
    # PyTorch does not support uint32 tensors, so we have to convert the indexes.
    return torch.tensor([es.src.astype('int64'), es.dst.astype('int64')])

  def output(self, name, values, *, type):
    '''Writes a list or Numpy array to disk.'''
    if hasattr(values, 'detach'):  # Turn PyTorch Tensors into Numpy arrays.
      values = values.detach().cpu().numpy()
    if isinstance(values, pa.lib.Array):
      if values.type != PA_TYPES[type]:
        values = values.cast(PA_TYPES[type])
    else:
      if not isinstance(values, list):
        values = list(values)
      values = pa.array(values, type=PA_TYPES[type], from_pandas=True)
    self.write_columns(name, type, {'value': values})

  def write_type(self, path, type):
    print('writing', type, 'to', path)
    os.makedirs(path, exist_ok=True)
    with open(path + '/type_name', 'w') as f:
      f.write(type)

  def write_columns(self, name, type, columns):
    path = self.datadir + '/' + self.outputs[name]
    self.write_type(path, type)
    schema = pa.schema([
        pa.field(name, a.type) for (name, a) in columns.items()])
    t = pa.Table.from_arrays(list(columns.values()), schema=schema)
    with pa.output_stream(path + '/data.arrow') as sink:
      writer = pa.RecordBatchFileWriter(sink, t.schema)
      batches = t.to_batches(max_chunksize=len(t))
      if batches:
        assert len(batches) == 1
        writer.write_batch(batches[0])
      writer.close()
    with open(path + '/_SUCCESS', 'w'):
      pass

  def output_vs(self, name, count):
    '''Writes a vertex set to disk. You just specify the vertex count.'''
    self.write_columns(name, 'VertexSet', {'sparkId': pa.array(range(count), pa.int64())})

  def output_es(self, name, edge_index):
    '''Writes an edge bundle specified as a 2xN matrix to disk.'''
    if hasattr(edge_index, 'detach'):
      edge_index = edge_index.detach().cpu().numpy()
    src, dst = edge_index
    self.write_columns(name, 'EdgeBundle', {
        'src': pa.array(src, pa.uint32()),
        'dst': pa.array(dst, pa.uint32()),
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
    import torch
    path = self.datadir + '/' + self.outputs[name]
    os.makedirs(path, exist_ok=True)
    torch.save(model, path + '/model.pt')
    self.output_scalar(name, description)

  def input_table(self, name):
    '''Reads the input table as a Pandas DataFrame.'''
    return pd.read_parquet(self.datadir + '/' + self.inputs[name])

  def output_table(self, name, df):
    '''Writes a Pandas DataFrame as a table.'''
    path = self.datadir + '/' + self.outputs[name]
    os.makedirs(path, exist_ok=True)
    df.to_parquet(path + '/data.parquet')
    with open(path + '/_SUCCESS', 'w') as f:
      pass

  def run_in_chroot(self):
    '''Runs this operation in a chroot environment.

    Python dependencies and inputs are mounted read-only.
    Outputs are moved back to the real data directory after the script terminates.

    We must be running as root for this to work. If in Docker, the container must be started with
    --cap-add=SYS_ADMIN and --security-opt apparmor:unconfined (depending on kernel version).
    If you start the container with --privileged that also covers these settings.
    '''
    import os
    import tempfile
    import shutil
    import subprocess
    import sys
    mounts = []

    def mount(src, dst):
      for m in mounts:
        if dst.startswith(m):  # Already mounted parent.
          return
      subprocess.run(['mkdir', '-p', dst], check=True)
      subprocess.run(['mount', '-o', 'bind', src, dst], check=True)
      subprocess.run(['mount', '--bind', '-o', 'remount,ro', src, dst], check=True)
      mounts.append(dst)
    # Prepare chroot environment.
    jail = tempfile.mkdtemp()
    ADD_TO_PYTHON_JAIL = os.environ.get('ADD_TO_PYTHON_JAIL')
    user_path = ADD_TO_PYTHON_JAIL.split(':') if ADD_TO_PYTHON_JAIL else []
    for pdir in user_path + sorted(sys.path):
      if os.path.isdir(pdir) and pdir.startswith('/'):
        mount(pdir, jail + pdir)
    for e in self.inputs.values():
      mount(f'{self.datadir}/{e}', f'{jail}/data/{e}')
    # Fork and jail the child.
    pid = os.fork()
    if pid == 0:  # Child. Continue the work in a chroot.
      os.chroot(jail)
      os.chdir('/')
      self.datadir = '/data'
    else:  # Parent. Wait for child and finish the work.
      _, error_and_reason = os.waitpid(pid, 0)
      if error_and_reason:
        error = error_and_reason // 256
        sys.exit(error)
      for m in mounts:
        subprocess.run(['umount', '-f', m], check=True)
      for e in self.outputs.values():
        shutil.move(f'{jail}/data/{e}', f'{self.datadir}/{e}')
      subprocess.run(['rm', '-rf', jail], check=True)
      sys.exit(0)
