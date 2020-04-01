#!/bin/bash -xue
# PyTorch Geometric installation is a nightmare because we have to get versions of
# PyTorch, PyArrow and the PyTorch Geometric libraries that are all built with compatible
# glibc++ and Boost libraries.
conda install -y pyarrow
conda install -y pytorch torchvision cpuonly -c pytorch
pip install --no-cache-dir torch-scatter==2.0.4+cpu -f https://pytorch-geometric.com/whl/torch-1.4.0.html
pip install --no-cache-dir torch-sparse==0.6.1+cpu -f https://pytorch-geometric.com/whl/torch-1.4.0.html
pip install --no-cache-dir torch-cluster==1.5.3+cpu -f https://pytorch-geometric.com/whl/torch-1.4.0.html
pip install --no-cache-dir torch-geometric==1.4.2
conda clean -a
