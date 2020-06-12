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
scalars = types.SimpleNamespace()
for fullname in op.inputs.keys():
  if '.' not in fullname:
    continue
  parent, name = fullname.split('.')
  if parent == 'vs':
    vs[name] = op.input(fullname)
  elif parent == 'es':
    es[name] = op.input(fullname)
  elif parent == 'scalars':
    setattr(scalars, name, op.input_scalar(fullname))
if 'edges-for-es' in op.inputs:
  edges = op.input('edges-for-es')
  es['src'] = edges.src
  es['dst'] = edges.dst
vs = pd.DataFrame(vs)
es = pd.DataFrame(es)

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
assert_no_extra(scalars.__dict__.keys(), 'scalars')
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
    elif parent == 'scalars':
      assert hasattr(scalars, name), f'scalars.{name} is not defined'
      op.output_scalar(fullname, getattr(scalars, name))
  except BaseException:
    import sys
    print(f'\nCould not output {fullname}:\n', file=sys.stderr)
    raise
