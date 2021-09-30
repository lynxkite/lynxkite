#!/bin/bash -xue
# Install PyTorch Geometric.

if ! pip list | grep torch-geometric | grep 2.0.1; then
  conda update -yn base conda
  conda install -y numpy pandas scikit-learn pyarrow
  conda install -y pytorch=1.9.0 cpuonly -c pytorch
  conda install -y pyg==2.0.1 -c pyg -c conda-forge
  conda clean -ya
fi
