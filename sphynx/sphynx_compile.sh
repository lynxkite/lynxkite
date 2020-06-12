#!/bin/bash -xue
# Compiles the sphynx server

cd $(dirname $0)
. sphynx_common.sh

# Make sure we use the newest versions
go get -u google.golang.org/grpc
go get -u github.com/xitongsys/parquet-go/writer
go get -u github.com/xitongsys/parquet-go-source/local

cd $GOPATH/src/$GO_PKG
go fmt $GO_PKG/lynxkite-sphynx
go get -v $GO_PKG/lynxkite-sphynx
