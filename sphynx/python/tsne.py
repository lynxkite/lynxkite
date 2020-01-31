'''Dimensionality reduction with t-SNE.'''
from sklearn.manifold import TSNE
from . import util

op = util.Op()
x = op.input('vector', type=util.DoubleVectorAttribute)
z = TSNE().fit_transform(x)
op.output('embedding', z, type=util.DoubleVectorAttribute)
