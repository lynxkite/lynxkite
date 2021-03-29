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
for name in op.inputs:
  globals()[name] = op.input_table(name)

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

# Write outputs.
for name in op.outputs:
  op.output_table(name, globals()[name])
