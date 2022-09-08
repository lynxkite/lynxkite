'''Uses CuGraph to implement some operations that we otherwise do with NetworKit.'''
from . import util
import cupy
import cugraph
import time
t0 = time.perf_counter()


def output_va(df, key):
  df = df.sort_values('vertex')[key].to_arrow()
  op.output('attr', df, type=util.DoubleAttribute)


def output_seg(df, key):
  df = df.sort_values('vertex')[['vertex', key]]
  ss = df[key]
  # This seems to hold, but is not documented.
  assert ss.max() == ss.nunique() - 1, 'Expected partitions to be contiguously numbered.'
  op.output_vs('partitions', ss.nunique())
  op.output_es('belongsTo', df.values.T)


op = util.Op()
nkop = op.params['op']
nkopt = op.params['options']
print(f'Running {nkop} on CUDA...')
# https://github.com/rapidsai/cugraph/issues/2670
es = op.input_cudf('es').astype({'src': 'int32', 'dst': 'int32'})
G = cugraph.Graph()
if 'weight' in op.inputs:
  es['w'] = op.input_cudf('weight')['values']
  G.from_cudf_edgelist(es, source='src', destination='dst', edge_attr='w', renumber=False)
else:
  G.from_cudf_edgelist(es, source='src', destination='dst', renumber=False)
if nkop == 'EstimateBetweenness':
  df = cugraph.betweenness_centrality(
      G, k=int(min(G.number_of_nodes(), nkopt['samples'])))
  output_va(df, 'betweenness_centrality')
elif nkop == 'KatzCentrality':
  df = cugraph.katz_centrality(G)
  output_va(df, 'katz_centrality')
elif nkop == 'PLM':
  df, _ = cugraph.louvain(G, resolution=nkopt['resolution'])
  output_seg(df, 'partition')
elif nkop == 'CoreDecomposition':
  df = cugraph.core_number(G)
  output_va(df, 'core_number')
elif nkop == 'ForceAtlas2':
  df = cugraph.force_atlas2(G)
  df = cupy.asnumpy(df.sort_values('vertex').drop(columns='vertex').values)
  op.output('attr', df, type=util.DoubleVectorAttribute)
else:
  assert False, f'Unexpected operation: {nkop}'
print(f'Finished {nkop} in {time.perf_counter() - t0} seconds.')
