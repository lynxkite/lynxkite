from torch_geometric.nn import GCNConv
import torch.nn.functional as F
import torch


class GCNConvNet(torch.nn.Module):
  def __init__(self, in_dim, out_dim):
    super(GCNConvNet, self).__init__()
    self.conv1 = GCNConv(in_dim, 16)
    self.conv2 = GCNConv(16, out_dim)
    self.is_classification = True

  def forward(self, data):
    x, edge_index = data.x, data.edge_index
    x = self.conv1(x, edge_index)
    x = F.relu(x)
    x = F.dropout(x, training=self.training)
    x = self.conv2(x, edge_index)
    return x


class GCNConvNetForRegression(torch.nn.Module):
  def __init__(self, in_dim):
    super(GCNConvNetForRegression, self).__init__()
    self.conv1 = GCNConv(in_dim, 16)
    self.conv2 = GCNConv(16, 16)
    self.lin = torch.nn.Linear(16, 1)
    self.is_classification = False

  def forward(self, data):
    x, edge_index = data.x, data.edge_index
    x = self.conv1(x, edge_index)
    x = F.relu(x)
    x = F.dropout(x, training=self.training)
    x = self.conv2(x, edge_index)
    x = F.relu(x)
    x = self.lin(x)
    return x.squeeze()
