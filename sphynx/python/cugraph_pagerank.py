'''Computes PageRank with CuGraph.'''
import cugraph
from . import util

op = util.Op()
es = op.input_cudf('es')
G = cugraph.Graph()
G.from_cudf_edgelist(es, source='src', destination='dst')
res = cugraph.pagerank(G, alpha=op.params['dampingFactor'], max_iter=op.params['iterations'])
res = res.sort_values('vertex').pagerank.to_arrow()
op.output('pagerank', res, type=util.DoubleAttribute)
