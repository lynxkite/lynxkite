#!/bin/bash -xue
# Compiles the sphynx server

cd $(dirname $0)
. sphynx_common.sh

go fmt $GO_PKG/lynxkite-sphynx $GO_PKG/networkit
pushd networkit
source build_env.sh
popd
go build -o .build/lynxkite-sphynx $GO_PKG/lynxkite-sphynx
cp -L networkit/libnetworkit.so .build/
