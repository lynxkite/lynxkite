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

# Get input.
es = op.input('es')
edges = torch.tensor([es.src, es.dst])
x = torch.from_numpy(op.input('features', type='DoubleVectorAttribute')).type(torch.float32)
model = op.input_model('model')
if model.forget:
  y_numpy = op.input('label')
  label = np.nan_to_num(y_numpy, copy=True)
  label_known = ~np.isnan(y_numpy)
  if model.is_classification:
    label = label.astype(np.int)
    label_for_input = np.zeros((label.size, model.num_classes))
    label_for_input[label_known, label[label_known]] = 1
    label_for_input = torch.from_numpy(label_for_input).type(torch.float32)
  else:
    label_for_input = torch.from_numpy(label).type(torch.float32).unsqueeze(1)
  label_known = torch.from_numpy(label_known).type(torch.float32).unsqueeze(1)
  x = torch.cat([x, label_for_input, label_known], 1)
data = Data(x=x, edge_index=edges).to(device)

# Run model.
model.eval()
with torch.no_grad():
  if model.is_classification:
    _, pred = model(data).max(dim=1)
  else:
    pred = model(data)

op.output('prediction', pred, type='DoubleAttribute')
