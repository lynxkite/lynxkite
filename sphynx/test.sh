#!/bin/bash -xue

cd $(dirname $0)
source sphynx_common.sh
cd networkit
source build_env.sh
cd -
export NETWORKIT_THREADS=4
go test "$@" $GO_PKG/lynxkite-sphynx
