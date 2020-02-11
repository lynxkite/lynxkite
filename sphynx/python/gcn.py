import numpy as np
import torch
from torch_geometric.data import Data
from torch.utils.data import DataLoader
from torch_geometric.nn import GCNConv
import torch.nn.functional as F
from . import util


class Net(torch.nn.Module):
  def __init__(self, in_dim, out_dim):
    super(Net, self).__init__()
    self.conv1 = GCNConv(in_dim, 16)
    self.conv2 = GCNConv(16, out_dim)

  def forward(self, data):
    x, edge_index = data.x, data.edge_index
    print('x', x.dtype)
    print('edge_index', edge_index.dtype)
    x = self.conv1(x, edge_index)
    x = F.relu(x)
    x = F.dropout(x, training=self.training)
    x = self.conv2(x, edge_index)
    return x


op = util.Op()
device = 'cuda' if torch.cuda.is_available() else 'cpu'
print(f'GCN running on {device}')
num_nodes = op.input_parquet('vs').metadata.num_rows
print('num_nodes:', num_nodes)
es = op.input('es')
edges = torch.tensor([es.src, es.dst])
x = torch.from_numpy(op.input('features', type='DoubleVectorAttribute')).type(torch.float32)
label = torch.from_numpy(op.input('label')).type(torch.long)
data = Data(x=x, edge_index=edges, y=label).to(device)
model = Net(in_dim=data.num_features, out_dim=2).to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
train_mask = op.input('trainMask') == 1
val_mask = op.input('valMask') == 1

# Train model.
model.train()
total_loss = 0
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

# train_mask
train_correct = pred[train_mask].eq(data.y[train_mask]).sum().item()
train_acc = train_correct / train_mask.sum()
val_correct = pred[val_mask].eq(data.y[val_mask]).sum().item()
val_acc = val_correct / val_mask.sum()

op.output('prediction', pred, type='DoubleAttribute')
op.output_scalar('trainAcc', train_acc)
op.output_scalar('valAcc', val_acc)
