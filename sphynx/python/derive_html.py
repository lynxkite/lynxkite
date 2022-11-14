'''Run user code.'''
import io
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

if op.params['mode'] == 'html':
  assert 'html' in globals(), 'Please save the output as "html".'
  sc = html
elif op.params['mode'] == 'matplotlib':
  import base64
  from matplotlib import pyplot as plt
  f = io.BytesIO()
  plt.savefig(f, format='svg')
  data = base64.b64encode(f.getvalue()).decode('ascii')
  sc = f'<img src="data:image/svg+xml;base64, {data}">'
elif op.params['mode'] == 'ipywidget':
  from ipywidgets.embed import embed_minimal_html
  f = io.StringIO()
  embed_minimal_html(f, views=None)
  sc = f.getvalue()
  # The widget code has to be loaded from a CDN.
  sc = sc.replace('embed-amd.js"', 'embed-amd.js" data-jupyter-widgets-cdn-only')
  # We patch the URL class because this is displayed via <iframe srcdoc="...">.
  sc = sc.replace('<body>', '''
    <body>
    <script>
      const RURL = URL;
    </script>
    <script>
      class URL extends RURL {
        constructor(url, base) {
          super(url, 'http://example.com/');
        }
      }
    </script>''')
op.output_scalar('sc', sc)