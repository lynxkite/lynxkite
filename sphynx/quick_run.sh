#!/bin/bash -xue
# Compiles and restarts the sphynx server, quickly.
# This does not recompile protobuf.

cd $(dirname $0)
. sphynx_common.sh

mkdir -p ../stage/sphynx/
rm ../stage/sphynx/lynxkite-sphynx || true
go build -o ../stage/sphynx/lynxkite-sphynx $GO_PKG/lynxkite-sphynx

pkill lynxkite-sphynx
# Sphynx restarts automatically.
