'''Dimensionality reduction with PCA.'''
from sklearn.decomposition import PCA
from . import util

op = util.Op()
x = op.input_vector('vector')
dim = op.params['dimensions']
z = PCA(n_components=dim).fit_transform(x)
op.output('embedding', z, type=util.DoubleVectorAttribute)
