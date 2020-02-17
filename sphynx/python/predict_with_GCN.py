'''Predicts a target variable with a Graph Convolutional Network using PyTorch Geometric.'''
import numpy as np
import torch
from torch_geometric.data import Data
from torch.utils.data import DataLoader
from torch_geometric.nn import GCNConv
import torch.nn.functional as F
from . import util
from . import models


op = util.Op()
device = 'cuda' if torch.cuda.is_available() else 'cpu'
print(f'GCN prediction running on {device}')
es = op.input('es')
edges = torch.tensor([es.src, es.dst])
x = torch.from_numpy(op.input('features', type='DoubleVectorAttribute')).type(torch.float32)
data = Data(x=x, edge_index=edges).to(device)
model = op.input_model('model')

# Generate and write output.
model.eval()
with torch.no_grad():
  if model.is_classification:
    _, pred = model(data).max(dim=1)
  else:
    pred = model(data)


op.output('prediction', pred, type='DoubleAttribute')
