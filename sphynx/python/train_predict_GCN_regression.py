'''Trains a Graph Convolutional Network using PyTorch Geometric
and predicts the missing values for regression tasks.'''
import numpy as np
import torch
from torch_geometric.data import Data
from torch.utils.data import DataLoader
from torch_geometric.nn import GCNConv
import torch.nn.functional as F
from . import util


class Net(torch.nn.Module):
  def __init__(self, in_dim):
    super(Net, self).__init__()
    self.conv1 = GCNConv(in_dim, 16)
    self.conv2 = GCNConv(16, 16)
    self.lin = torch.nn.Linear(16, 1)

  def forward(self, data):
    x, edge_index = data.x, data.edge_index
    x = self.conv1(x, edge_index)
    x = F.relu(x)
    x = F.dropout(x, training=self.training)
    x = self.conv2(x, edge_index)
    x = F.relu(x)
    x = self.lin(x)
    return x.squeeze()


op = util.Op()
torch.manual_seed(op.params['seed'])
device = 'cuda' if torch.cuda.is_available() else 'cpu'
print(f'GCN running on {device}')
es = op.input('es')
edges = torch.tensor([es.src, es.dst])
x = torch.from_numpy(op.input('features', type='DoubleVectorAttribute')).type(torch.float32)
label = torch.from_numpy(op.input('label')).type(torch.float32)
data = Data(x=x, edge_index=edges, y=label).to(device)
model = Net(in_dim=data.num_features).to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
train_mask = op.input('trainMask') == 1
val_mask = op.input('valMask') == 1

# Train model.
model.train()
total_loss = 0
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

train_loss = F.mse_loss(out[train_mask], data.y[train_mask]).item()
val_loss = F.mse_loss(out[val_mask], data.y[val_mask]).item()

op.output('prediction', pred, type='DoubleAttribute')
op.output_scalar('trainMSE', train_loss)
op.output_scalar('valMSE', val_loss)
