#!/bin/bash -xue
# Downloads and builds NetworKit and creates symlinks in this directory for the Go build.

if [[ ! -e ~/networkit ]]; then
  pushd ~
  # Temporarily using a fork for some fixes, but eventually should be:
  # git clone https://github.com/networkit/networkit.git --branch 7.1 --single-branch --depth 1
  NK_VERSION=67eab6048fdb6bb61dec4be3add68fef1968e582
  git clone https://github.com/darabos/networkit.git && cd networkit && git checkout $NK_VERSION && cd ..
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
export LIBRARY_PATH="$(pwd)"
export CGO_LDFLAGS="-lnetworkit"
