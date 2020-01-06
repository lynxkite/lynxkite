#!/bin/bash -xue
# Downloads the necessary packages.
# Add a line to this file if you need more.

cd $(dirname $0)
. sphynx_common.sh

go get -u google.golang.org/grpc
go get -u github.com/golang/protobuf/protoc-gen-go
go get -u github.com/xitongsys/parquet-go/writer
go get -u github.com/xitongsys/parquet-go-source/local

