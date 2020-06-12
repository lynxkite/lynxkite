'''Dimensionality reduction with t-SNE.'''
from sklearn.manifold import TSNE
from . import util

op = util.Op()
x = op.input_vector('vector')
dim = op.params['dimensions']
z = TSNE(n_components=dim, perplexity=op.params['perplexity']).fit_transform(x)
op.output('embedding', z, type=util.DoubleVectorAttribute)
