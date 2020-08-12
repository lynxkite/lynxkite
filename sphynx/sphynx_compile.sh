#!/bin/bash -xue
# Compiles the sphynx server

cd $(dirname $0)
. sphynx_common.sh

go fmt $GO_PKG/lynxkite-sphynx
go build -o .build/lynxkite-sphynx $GO_PKG/lynxkite-sphynx
