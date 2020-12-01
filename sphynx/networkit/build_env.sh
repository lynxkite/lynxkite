#!/bin/bash -xue
# Downloads and builds NetworKit and creates symlinks in this directory for the Go build.

if [[ ! -e ~/networkit ]]; then
  pushd ~
  git clone https://github.com/networkit/networkit.git --branch 7.1 --single-branch --depth 1
  mkdir networkit/build
  cd networkit/build
  git submodule update --init
  cmake ..
  make -j4
  popd
fi
if [[ ! -e include ]]; then ln -s ~/networkit/include .; fi
if [[ ! -e cpp ]]; then ln -s ~/networkit/networkit/cpp .; fi
if [[ ! -e tlx ]]; then ln -s ~/networkit/extlibs/tlx/tlx .; fi
if [[ ! -e libnetworkit.so ]]; then ln -s ~/networkit/build/libnetworkit.so .; fi
export CPLUS_INCLUDE_PATH="$(pwd)/include"
export LD_LIBRARY_PATH="$(pwd)"
export CGO_LDFLAGS="-lnetworkit"
