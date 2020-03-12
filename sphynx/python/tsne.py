'''Dimensionality reduction with t-SNE.'''
from sklearn.manifold import TSNE
from . import util

op = util.Op()
x = op.input_vector('vector')
z = TSNE(perplexity=op.params['perplexity']).fit_transform(x)
op.output('embedding', z, type=util.DoubleTuple2Attribute)
