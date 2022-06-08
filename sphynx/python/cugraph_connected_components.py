'''Finds connected components with CuGraph.'''
import cugraph
import time
from . import util
t0 = time.perf_counter()


def output_seg(df, key):
  df = df.sort_values('vertex')[['vertex', key]]
  # Mapping to contiguous IDs.
  ids = {v: i for (i, v) in enumerate(df[key].unique().values_host)}
  df[key] = df[key].replace(ids)
  op.output_vs('segments', len(ids))
  op.output_es('belongsTo', df.values.T)


print(f'Running connected components on CUDA...')
op = util.Op()
es = op.input_cudf('es')
G = cugraph.Graph()
G.from_cudf_edgelist(es, source='src', destination='dst')
df = cugraph.connected_components(G)
output_seg(df, 'labels')
print(f'Finished connected components in {time.perf_counter() - t0} seconds.')
