#!/bin/bash -xue
# Compiles the sphynx server

cd $(dirname $0)
. sphynx_common.sh

cd $GOPATH/src/$GO_PKG
go fmt $GO_PKG/server
go get -v $GO_PKG/server
