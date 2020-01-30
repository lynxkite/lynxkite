'''Dimensionality reduction with t-SNE.'''
from sklearn.manifold import TSNE
from . import util

op = util.Op()
x = op.input('vector')
z = TSNE().fit_transform(x)
op.output_tuple2('embedding', z)
