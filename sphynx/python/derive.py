'''Run user code.'''
import numpy as np
import pandas as pd
import types
from . import util

op = util.Op()
print('Running derive python', op.params, op.inputs, op.outputs)
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
exec(op.params['code'])

# Save outputs.
typenames = {
    f['parent'] + '.' + f['name']: f['tpe']['typename'] for f in op.params['outputFields']}
typemapping = {
    'String': util.StringAttribute,
    'Double': util.DoubleAttribute,
}
for fullname in op.outputs.keys():
  if '.' not in fullname:
    continue
  parent, name = fullname.split('.')
  if parent == 'vs':
    op.output(fullname, vs[name], type=typemapping[typenames[fullname]])
  elif parent == 'es':
    op.output(fullname, es[name], type=typemapping[typenames[fullname]])
  elif parent == 'scalars':
    op.output_scalar(fullname, getattr(scalars, name))
