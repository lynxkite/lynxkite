'''Computes PageRank with CuGraph.'''
import cugraph
from . import util

op = util.Op()
es = op.input_cudf('es').astype({'src': 'int32', 'dst': 'int32'})
G = cugraph.Graph()
G.from_cudf_edgelist(es, source='src', destination='dst', renumber=False)
res = cugraph.pagerank(G, alpha=op.params['dampingFactor'], max_iter=op.params['iterations'])
res = res.sort_values('vertex').pagerank.to_arrow()
op.output('pagerank', res, type=util.DoubleAttribute)
