'''Makes some PyTorch-Geometric datasets available in LynxKite.'''
import torch_geometric.datasets as ds
from . import util

op = util.Op()
name = op.params["name"]
print('loading dataset', name)
if name == 'Karate Club':
  data = ds.KarateClub().data
else:
  data = ds.Planetoid('/tmp/' + name, name).data

op.output_vs('vs', len(data.x))
op.output_es('es', data.edge_index)
op.output('x', data.x, type=util.DoubleVectorAttribute)
op.output('y', data.y, type=util.DoubleAttribute)
