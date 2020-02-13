#!/bin/bash -xue
# PyTorch Geometric installation is a nightmare because we have to get versions of
# PyTorch, PyArrow and the PyTorch Geometric libraries that are all built with compatible
# glibc++ and Boost libraries.
conda install -y -c conda-forge boost pyarrow
conda install -y -c pytorch pytorch
git clone --depth 1 --branch 1.4.5 https://github.com/rusty1s/pytorch_cluster.git
git clone --depth 1 --branch 2.0.2 https://github.com/rusty1s/pytorch_scatter.git
git clone --depth 1 --branch 0.4.4 https://github.com/rusty1s/pytorch_sparse.git
sed -i 's/extra_compile_args = .*/extra_compile_args = ["-D_GLIBCXX_USE_CXX11_ABI=1"]/' pytorch_cluster/setup.py
sed -i 's/extra_compile_args = ..cxx.: ../extra_compile_args = {"cxx": ["-D_GLIBCXX_USE_CXX11_ABI=1"]/' pytorch_scatter/setup.py
sed -i 's/extra_compile_args = ..cxx.: ../extra_compile_args = {"cxx": ["-D_GLIBCXX_USE_CXX11_ABI=1"]/' pytorch_sparse/setup.py
pip install -e ./pytorch_cluster ./pytorch_scatter ./pytorch_sparse
pip install torch-geometric==1.4.1
