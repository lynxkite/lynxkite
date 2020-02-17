'''Trains a Graph Convolutional Network using PyTorch Geometric.'''
import numpy as np
import torch
from torch_geometric.data import Data
from torch.utils.data import DataLoader
from torch_geometric.nn import GCNConv
import torch.nn.functional as F
from . import util
from . import models

op = util.Op()
torch.manual_seed(op.params['seed'])
device = 'cuda' if torch.cuda.is_available() else 'cpu'
print(f'GCN running on {device}')
es = op.input('es')
edges = torch.tensor([es.src, es.dst])
x = torch.from_numpy(op.input('features', type='DoubleVectorAttribute')).type(torch.float32)
y_numpy = op.input('label')
train_mask = ~np.isnan(y_numpy)
label = torch.from_numpy(y_numpy).type(torch.float32)
data = Data(x=x, edge_index=edges, y=label).to(device)
model = models.GCNConvNetForRegression(in_dim=data.num_features).to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

# Train model.
model.train()
for epoch in range(op.params['iterations']):
  optimizer.zero_grad()
  out = model(data)
  loss = F.mse_loss(out[train_mask], data.y[train_mask])
  loss.backward()
  optimizer.step()
  print('epoch', epoch, 'loss', loss.item())

# Generate and write output.
model.eval()
with torch.no_grad():
  pred = model(data)

train_mse = F.mse_loss(pred[train_mask], data.y[train_mask]).item()

op.output_model('model', model, 'GCN classifier')
op.output_scalar('trainMSE', train_mse)
