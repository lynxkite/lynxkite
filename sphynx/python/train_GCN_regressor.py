'''Trains a Graph Convolutional Network using PyTorch Geometric.'''
import numpy as np
import torch
from torch_geometric.data import Data
import torch.nn.functional as F
from . import util
from . import models

op = util.Op()
seed = op.params['seed']
device = 'cuda' if torch.cuda.is_available() else 'cpu'
torch.manual_seed(seed)
np.random.seed(seed)
print(f'GCN running on {device}')


def get_feature_matrix(train_mask, batch_size, x, y_numpy):
  train_batch = np.random.choice(np.where(train_mask)[0], batch_size, replace=False)
  in_train_batch = np.zeros_like(y_numpy)
  in_train_batch[train_batch] = 1
  label_for_input = np.nan_to_num(y_numpy, copy=True)
  label_for_input[train_batch] = 0
  label_known = np.copy(train_mask)
  label_known[train_batch] = 0
  label_for_input = torch.from_numpy(label_for_input).type(torch.float32).unsqueeze(1)
  label_known = torch.from_numpy(label_known).type(torch.float32).unsqueeze(1)
  return in_train_batch == 1, torch.cat([x, label_for_input, label_known], 1)


# Get graph, features and target label.
edges = op.input_torch_edges('es')
x = torch.from_numpy(op.input_vector('features')).type(torch.float32)
y_numpy = op.input('label')
label = torch.from_numpy(y_numpy).type(torch.float32)
train_mask = ~np.isnan(y_numpy)
batch_size = min(op.params['batch_size'], train_mask.sum())
forget = op.params['forget']
lr = op.params['learning_rate']
num_conv_layers = op.params['num_conv_layers']
conv_op = op.params['conv_op']
hidden_size = op.params['hidden_size']


# Define model.
in_dim = x.size()[1] + 2 if forget else x.size()[1]
model = models.GCNConvNetForRegression(
    in_dim=in_dim,
    num_conv_layers=num_conv_layers,
    conv_op=conv_op,
    hidden_size=hidden_size,
    forget=forget).to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=lr)

# Train model.
model.train()
for epoch in range(op.params['iterations']):
  if forget:
    batch_train_mask, batch_x = get_feature_matrix(train_mask, batch_size, x, y_numpy)
  else:
    batch_train_mask, batch_x = train_mask, x
  data = Data(x=batch_x, edge_index=edges, y=label).to(device)
  optimizer.zero_grad()
  out = model(data)
  loss = F.mse_loss(out[batch_train_mask], data.y[batch_train_mask])
  loss.backward()
  optimizer.step()
  if epoch % 100 == 0:
    print('epoch', epoch, 'loss', loss.item())

# Measure performance
model.eval()
with torch.no_grad():
  pred = model(data)
train_mse = F.mse_loss(pred[batch_train_mask], data.y[batch_train_mask]).item()

op.output_model('model', model, 'GCN regressor')
op.output_scalar('trainMSE', train_mse)
