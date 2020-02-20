from torch_geometric.nn import GCNConv
from torch_geometric.nn import GatedGraphConv

import torch.nn.functional as F
import torch


class GCNConvNet(torch.nn.Module):
  def __init__(self, in_dim, out_dim, forget, num_classes, num_conv_layers, hidden_size, conv_op):
    super(GCNConvNet, self).__init__()
    self.is_classification = True
    self.forget = forget
    self.num_conv_layers = num_conv_layers
    self.num_classes = num_classes
    self.conv_op = conv_op
    if conv_op == "GCNConv":
      self.conv_layers = []
      sizes = [in_dim] + [hidden_size] * (num_conv_layers - 1) + [out_dim]
      zipped_sizes = zip(sizes[:-1], sizes[1:])
      for (i, (s1, s2)) in enumerate(zipped_sizes):
        conv = GCNConv(s1, s2)
        self.conv_layers.append(conv)
        self.add_module(f'conv{i}', conv)
    elif conv_op == "GatedGraphConv":
      self.gated_conv = GatedGraphConv(hidden_size, num_layers=num_conv_layers)
      self.lin1 = torch.nn.Linear(in_dim, hidden_size)
      self.lin2 = torch.nn.Linear(hidden_size, num_classes)
    else:
      raise Exception(f'Unknown conovlution operator: {conv_op}')

  def forward(self, data):
    x, edge_index = data.x, data.edge_index
    if self.conv_op == "GCNConv":
      for conv in self.conv_layers[:-1]:
        x = conv(x, edge_index)
        x = F.relu(x)
        x = F.dropout(x, training=self.training)
      conv = self.conv_layers[-1]
      x = conv(x, edge_index)
    elif self.conv_op == "GatedGraphConv":
      x = self.lin1(x)
      x = F.relu(x)
      x = self.gated_conv(x, edge_index)
      x = self.lin2(x)
    return x


class GCNConvNetForRegression(torch.nn.Module):
  def __init__(self, in_dim, forget, num_conv_layers, hidden_size, conv_op):
    super(GCNConvNetForRegression, self).__init__()
    self.is_classification = False
    self.forget = forget
    self.num_conv_layers = num_conv_layers
    self.conv_op = conv_op
    if self.conv_op == "GCNConv":
      self.conv_layers = []
      sizes = [in_dim] + [hidden_size] * num_conv_layers
      zipped_sizes = zip(sizes[:-1], sizes[1:])
      # To make the module aware of its parameters, we set them as attributes.
      for (i, (s1, s2)) in enumerate(zipped_sizes):
        conv = GCNConv(s1, s2)
        self.conv_layers.append(conv)
        self.add_module(f'conv{i}', conv)
    elif self.conv_op == "GatedGraphConv":
      self.gated_conv = GatedGraphConv(hidden_size, num_layers=num_conv_layers)
      self.lin1 = torch.nn.Linear(in_dim, hidden_size)
      self.lin2 = torch.nn.Linear(hidden_size, 1)
    else:
      raise Exception(f'Unknown conovlution operator: {conv_op}')
    self.lin = torch.nn.Linear(hidden_size, 1)

  def forward(self, data):
    x, edge_index = data.x, data.edge_index
    if self.conv_op == "GCNConv":
      for conv in self.conv_layers[:-1]:
        x = conv(x, edge_index)
        x = F.relu(x)
        x = F.dropout(x, training=self.training)
      conv = self.conv_layers[-1]
      x = conv(x, edge_index)
      x = F.relu(x)
      x = self.lin(x)
    elif self.conv_op == "GatedGraphConv":
      x = self.lin1(x)
      x = F.relu(x)
      x = self.gated_conv(x, edge_index)
      x = self.lin2(x)
    return x.squeeze()
