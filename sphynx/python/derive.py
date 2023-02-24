'''Run user code.'''
import numpy as np
import pandas as pd
import os
import types
from . import util

op = util.Op()
if os.environ.get('SPHYNX_CHROOT_PYTHON') == 'yes':
  op.run_in_chroot()

# Load inputs.
vs = {}
es = {}
graph_attributes = types.SimpleNamespace()
for fullname in op.inputs.keys():
  if '.' not in fullname:
    continue
  parent, name = fullname.split('.')
  if parent == 'vs':
    vs[name] = op.input(fullname)
  elif parent == 'es':
    es[name] = op.input(fullname)
  elif parent == 'graph_attributes':
    setattr(graph_attributes, name, op.input_scalar(fullname))
if 'edges-for-es' in op.inputs:
  edges = op.input('edges-for-es')
  es['src'] = edges.src
  es['dst'] = edges.dst
vs = pd.DataFrame(vs)
es = pd.DataFrame(es)


def ai(query, output_schema):
  # TODO: Add a DeriveTableFromGraph operation.
  print(query, output_schema)
  from .llm_pandas_on_graph import pandas_on_graph
  df = pandas_on_graph(nodes=vs, edges=es, query=query, output_schema=output_schema)
  print(df)


# Execute user code.
try:
  code = compile(op.params['code'], 'user code', 'exec')
  exec(code)
except BaseException:
  # Hide this file from the traceback.
  import traceback
  import sys
  a, b, c = sys.exc_info()
  traceback.print_exception(a, b, c.tb_next)
  sys.exit(1)


def assert_no_extra(columns, name):
  inputs = set(f['name'] for f in op.params['inputFields'] if f['parent'] == name)
  outputs = set(f['name'] for f in op.params['outputFields'] if f['parent'] == name)
  extra = set(columns) - inputs - outputs
  if extra:
    import sys
    print('Undeclared output found: ' + ', '.join(name + '.' + e for e in extra), file=sys.stderr)
    sys.exit(1)


assert_no_extra(vs.columns, 'vs')
assert_no_extra(set(es.columns) - set(['src', 'dst']), 'es')
assert_no_extra(graph_attributes.__dict__.keys(), 'graph_attributes')
# Save outputs.
typenames = {
    f['parent'] + '.' + f['name']: f['tpe']['typename'] for f in op.params['outputFields']}
typemapping = {
    'String': util.StringAttribute,
    'Double': util.DoubleAttribute,
    'Vector[Double]': util.DoubleVectorAttribute,
}
for fullname in op.outputs.keys():
  if '.' not in fullname:
    continue
  parent, name = fullname.split('.')
  try:
    if parent == 'vs':
      assert name in vs.columns, f'vs does not have a column named "{name}"'
      op.output(fullname, vs[name], type=typemapping[typenames[fullname]])
    elif parent == 'es':
      assert name in es.columns, f'es does not have a column named "{name}"'
      op.output(fullname, es[name], type=typemapping[typenames[fullname]])
    elif parent == 'graph_attributes':
      assert hasattr(graph_attributes, name), f'graph_attributes.{name} is not defined'
      op.output_scalar(fullname, getattr(graph_attributes, name))
  except BaseException:
    import sys
    print(f'\nCould not output {fullname}:\n', file=sys.stderr)
    raise
