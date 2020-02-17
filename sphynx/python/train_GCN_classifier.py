'''Trains a Graph Convolutional Network using PyTorch Geometric.'''
import numpy as np
import torch
from torch_geometric.data import Data
from torch.utils.data import DataLoader
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
label = torch.from_numpy(y_numpy).type(torch.long)
num_classes = torch.max(label).item() + 1
data = Data(x=x, edge_index=edges, y=label).to(device)
model = models.GCNConvNet(in_dim=data.num_features, out_dim=num_classes).to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

# Train model.
model.train()
for epoch in range(op.params['iterations']):
  optimizer.zero_grad()
  out = model(data)
  loss = F.cross_entropy(out[train_mask], data.y[train_mask])
  loss.backward()
  optimizer.step()
  print('epoch', epoch, 'loss', loss.item())

# Generate and write output.
model.eval()
with torch.no_grad():
  _, pred = model(data).max(dim=1)

train_correct = pred[train_mask].eq(data.y[train_mask]).sum().item()
train_acc = train_correct / train_mask.sum()

op.output_model('model', model, 'GCN classifier')
op.output_scalar('trainAcc', train_acc)
