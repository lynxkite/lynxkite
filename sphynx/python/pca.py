'''Dimensionality reduction with PCA.'''
from sklearn.decomposition import PCA
from . import util

op = util.Op()
x = op.input_vector('vector')
dim = op.params['dimensions']
z = PCA(n_components=dim).fit_transform(x)
if dim == 2:
  op.output('embedding', z, type=util.DoubleTuple2Attribute)
else:
  op.output('embedding', z, type=util.DoubleVectorAttribute)
