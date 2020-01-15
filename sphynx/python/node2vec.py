'''Creates a node embedding using PyTorch Geometric.'''
import json
import numpy as np
import os
import pygob  # TODO: Switch to Parquet.
import sys
import torch
from torch.utils.data import DataLoader
from torch_geometric.nn import Node2Vec


def log(*args):
  print(*args, file=sys.stderr)


num_nodes = int(sys.argv[1])
embedding_dim = int(sys.argv[2])
es = sys.argv[3]
device = 'cuda' if torch.cuda.is_available() else 'cpu'
log(f'node2vec in {embedding_dim} dimensions with {num_nodes} nodes running on {device}')
log(f'reading {es}')

with open(es + '/Src', 'rb') as f:
  srcs = pygob.load(f.read())
with open(es + '/Dst', 'rb') as f:
  dsts = pygob.load(f.read())

# We need to add loop edges for vertices with no outgoing edges.
# https://github.com/rusty1s/pytorch_cluster/issues/45
index, counts = np.unique(srcs, return_counts=True)
degree = np.zeros(num_nodes)
degree[index] = counts
deadends = (degree == 0).nonzero()
srcs.extend(deadends)
dsts.extend(deadends)

edges = torch.tensor([srcs, dsts])

loader = DataLoader(torch.arange(num_nodes), batch_size=128, shuffle=True)
model = Node2Vec(num_nodes, embedding_dim=embedding_dim, walk_length=20,
                 context_size=10, walks_per_node=10)
model, edges = model.to(device), edges.to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)


def train():
  model.train()
  total_loss = 0
  for subset in loader:
    optimizer.zero_grad()
    loss = model.loss(edges, subset.to(device))
    loss.backward()
    optimizer.step()
    total_loss += loss.item()
  return total_loss / len(loader)


for epoch in range(10):
  loss = train()
  log('epoch', epoch, 'loss', loss)

model.eval()
with torch.no_grad():
  z = model(torch.arange(num_nodes, device=device))

# TODO: Switch to Parquet.
print(json.dumps({
    'pagerank': {'TypeName': 'DoubleAttribute', 'Data': {
      'Defined': [True] * num_nodes,
      'Values': z.T[0].tolist(),
      }
    }
}))
