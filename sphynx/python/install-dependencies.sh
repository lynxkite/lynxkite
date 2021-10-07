#!/bin/bash -xue
# Install PyTorch Geometric.

cd $(dirname $0)
if ! pip list | grep torch-geometric | grep 2.0.1; then
  conda update env --file env.yml
fi
