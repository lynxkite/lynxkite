#!/bin/bash -xue

ln -s ~/networkit/include . || true
ln -s ~/networkit/networkit/cpp . || true
ln -s ~/networkit/extlibs/tlx/tlx . || true
ln -s ~/networkit/build/libnetworkit.so . || true
CPLUS_INCLUDE_PATH="$(pwd)/include" \
LD_LIBRARY_PATH="$(pwd)" \
CGO_LDFLAGS="-lnetworkit" \
  go test "$@"
