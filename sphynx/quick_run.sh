#!/bin/bash -xue
# Compiles and restarts the sphynx server, quickly.
# This does not recompile protobuf.

cd $(dirname $0)
. sphynx_common.sh

mkdir -p ../stage/sphynx/
rm ../stage/sphynx/lynxkite-sphynx || true
./sphynx_compile.sh
cp .build/lynxkite-sphynx ../stage/sphynx/lynxkite-sphynx

pkill lynxkite-sphynx
# Sphynx restarts automatically.
