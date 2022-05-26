'''Uses CuGraph to implement some operations that we otherwise do with NetworKit.'''
import cugraph
from . import util

op = util.Op()
nkop = op.params['op']
nkopt = op.params['options']
print(f'Running {nkop} on CUDA...')
es = op.input_cudf('es')
G = cugraph.Graph()
G.from_cudf_edgelist(es, source='src', destination='dst')
if nkop == 'EstimateBetweenness':
  res = cugraph.betweenness_centrality(
      G, k=int(min(G.number_of_nodes(), nkopt['samples'])))
  key = 'betweenness_centrality'
else:
  assert False, f'Unexpected operation: {nkop}'
res = res.sort_values('vertex')[key].to_arrow()
op.output('attr', res, type=util.DoubleAttribute)
