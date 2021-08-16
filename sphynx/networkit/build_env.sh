#!/bin/bash -xue
# Downloads and builds NetworKit and creates symlinks in this directory for the Go build.

if [[ ! -e ~/networkit ]]; then
  pushd ~
  git clone https://github.com/networkit/networkit.git --branch release-7.1 --depth 1
  cd networkit
  mkdir build && cd build
  git submodule update --init
  cmake -DNETWORKIT_STATIC=ON ..
  make -j5
  popd
fi
if [[ ! -e include ]]; then ln -s ~/networkit/include .; fi
if [[ ! -e cpp ]]; then ln -s ~/networkit/networkit/cpp .; fi
if [[ ! -e tlx ]]; then ln -s ~/networkit/extlibs/tlx/tlx .; fi
if [[ ! -e libnetworkit.a ]]; then ln -s ~/networkit/build/libnetworkit.a .; fi
export CPLUS_INCLUDE_PATH="$(pwd)/include"
export LD_LIBRARY_PATH="$(pwd)"
export LIBRARY_PATH="$(pwd)"
export CGO_CXXFLAGS="-Wno-deprecated-declarations"
export CGO_LDFLAGS="-static -lnetworkit -fopenmp"
