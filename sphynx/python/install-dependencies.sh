#!/bin/bash -xue
# Install PyTorch Geometric.

if ! pip list | grep torch-geometric | grep 1.4.2; then
  # Conda update had to be added because otherwise Conda would downgrade Node.js
  # to version 6 and cause failures later on.
  conda update -yn base conda pip
  conda install -y pyarrow
  conda install -y pytorch=1.4.0 torchvision cpuonly -c pytorch
  yes | pip install --no-cache-dir torch-scatter==2.0.4+cpu -f https://pytorch-geometric.com/whl/torch-1.4.0.html
  yes | pip install --no-cache-dir torch-sparse==0.6.1+cpu -f https://pytorch-geometric.com/whl/torch-1.4.0.html
  yes | pip install --no-cache-dir torch-cluster==1.5.3+cpu -f https://pytorch-geometric.com/whl/torch-1.4.0.html
  yes | pip install --no-cache-dir torch-geometric==1.4.2
  conda clean -ya
fi
