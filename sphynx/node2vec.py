import os
import sys
import pygob

es = sys.argv[1]
output = sys.argv[2]
print('es', es)
print('output', output)

with open(es + '/typename', 'rb') as f:
  print('typename', pygob.load(f.read()))
with open(es + '/Src', 'rb') as f:
  print('src', pygob.load(f.read()))
with open(es + '/Dst', 'rb') as f:
  print('dst', pygob.load(f.read()))

os.mkdir(output)
with open(output + '/typename', 'wb') as f:
  f.write(pygob.dump('DoubleAttribute'))
with open(output + '/Defined', 'wb') as f:
  f.write(pygob.dump([True, True, True, True]))
with open(output + '/Values', 'wb') as f:
  f.write(pygob.dump([1.2, 1.3, 1.4, 1.5]))
os.exit(0)
import os.path as osp

import torch
from torch.utils.data import DataLoader
import matplotlib.pyplot as plt
from sklearn.manifold import TSNE
from torch_geometric.datasets import Planetoid
from torch_geometric.nn import Node2Vec

dataset = 'Cora'
path = osp.join(osp.dirname(osp.realpath(__file__)), '..', 'data', dataset)
dataset = Planetoid(path, dataset)
data = dataset[0]
print(data.num_nodes)
print(data.edge_index.size())
print(dir(data))
asd
loader = DataLoader(torch.arange(data.num_nodes), batch_size=128, shuffle=True)

device = 'cuda' if torch.cuda.is_available() else 'cpu'
model = Node2Vec(data.num_nodes, embedding_dim=128, walk_length=20,
                 context_size=10, walks_per_node=10)
model, data = model.to(device), data.to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)


def train():
    model.train()
    total_loss = 0
    for subset in loader:
        optimizer.zero_grad()
        loss = model.loss(data.edge_index, subset.to(device))
        loss.backward()
        optimizer.step()
        total_loss += loss.item()
    return total_loss / len(loader)


def test():
    model.eval()
    with torch.no_grad():
        z = model(torch.arange(data.num_nodes, device=device))
    acc = model.test(z[data.train_mask], data.y[data.train_mask],
                     z[data.test_mask], data.y[data.test_mask], max_iter=150)
    return acc


for epoch in range(1, 10):
    loss = train()
    print('Epoch: {:02d}, Loss: {:.4f}'.format(epoch, loss))
acc = test()
print('Accuracy: {:.4f}'.format(acc))


def plot_points(colors):
    model.eval()
    with torch.no_grad():
        z = model(torch.arange(data.num_nodes, device=device))
        z = TSNE(n_components=2).fit_transform(z.cpu().numpy())
        y = data.y.cpu().numpy()

    plt.figure(figsize=(8, 8))
    for i in range(dataset.num_classes):
        plt.scatter(z[y == i, 0], z[y == i, 1], s=20, color=colors[i])
    plt.axis('off')
    plt.show()


colors = [
    '#ffc0cb', '#bada55', '#008080', '#420420', '#7fe5f0', '#065535', '#ffd700'
]
plot_points(colors)
