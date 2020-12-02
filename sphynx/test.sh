#!/bin/bash -xue

cd $(dirname $0)
source sphynx_common.sh
cd networkit
source build_env.sh
cd -
go test "$@" $GO_PKG/lynxkite-sphynx
