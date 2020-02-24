'''Run user code.'''
import pandas as pd
import types
from . import util

op = util.Op()
print('derive python', op.params, op.inputs, op.outputs)
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
vs = pd.DataFrame(vs)
es = pd.DataFrame(es)

exec(op.params['code'])

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
    op.output(fullname, getattr(vs, name), type=typemapping[typenames[fullname]])
  elif parent == 'es':
    op.output(fullname, getattr(es, name), type=typemapping[typenames[fullname]])
  elif parent == 'scalars':
    op.output_scalar(fullname, getattr(scalars, name))
