#!/bin/bash -xue

ln -s ~/networkit/include . || true
ln -s ~/networkit/networkit/cpp . || true
ln -s ~/networkit/extlibs/tlx/tlx . || true
ln -s ~/networkit/build/libnetworkit.so . || true
export CPLUS_INCLUDE_PATH="$(pwd)/include"
export LD_LIBRARY_PATH="$(pwd)"
export CGO_LDFLAGS="-lnetworkit"
