'''Run user code to create a new graph.'''
import numpy as np
import pandas as pd
import os
import types
from . import util

op = util.Op()
if os.environ.get('SPHYNX_CHROOT_PYTHON') == 'yes':
  op.run_in_chroot()

# Load inputs.
scalars = types.SimpleNamespace()
vs = pd.DataFrame()
es = pd.DataFrame()

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

# Save outputs.
op.output_vs('vertices', len(vs))
field_names = {f['parent'] + '.' + f['name'] for f in op.params['outputFields']}
if 'src' in es.columns and 'dst' in es.columns:
  op.output_es('edges', np.stack([es.src, es.dst]))
else:
  op.output_es('edges', ([], []))
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
