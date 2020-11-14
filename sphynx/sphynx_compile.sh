#!/bin/bash -xue
# Compiles the sphynx server

cd $(dirname $0)
. sphynx_common.sh

go fmt $GO_PKG/lynxkite-sphynx $GO_PKG/networkit
cd networkit
source build_env.sh
cd -
go build -o .build/lynxkite-sphynx $GO_PKG/lynxkite-sphynx
cp -L networkit/libnetworkit.so .build/
