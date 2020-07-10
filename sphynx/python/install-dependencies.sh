#!/bin/bash -xue
# Install PyTorch Geometric.

if pip list | grep torch-geometric | grep 1.4.2; then
  echo 'PyTorch Geometric is already up to date.'
  exit 0
fi
# Conda update had to be added because otherwise Conda would downgrade Node.js
# to version 6 and cause failures later on.
conda update -yn base conda pip
conda install -y pyarrow
conda install -y pytorch=1.4.0 torchvision cpuonly -c pytorch
pip install -y --no-cache-dir torch-scatter==2.0.4+cpu -f https://pytorch-geometric.com/whl/torch-1.4.0.html
pip install -y --no-cache-dir torch-sparse==0.6.1+cpu -f https://pytorch-geometric.com/whl/torch-1.4.0.html
pip install -y --no-cache-dir torch-cluster==1.5.3+cpu -f https://pytorch-geometric.com/whl/torch-1.4.0.html
pip install -y --no-cache-dir torch-geometric==1.4.2
conda clean -ya
