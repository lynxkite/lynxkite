#!/bin/bash -xue
# Compiles and restarts the sphynx server, quickly.
# This does not recompile protobuf and does not download
# the newest package versions

export SPARK_VERSION=something
KITE_SITE_CONFIG=${KITE_SITE_CONFIG:-$HOME/.kiterc}

. $KITE_SITE_CONFIG

cd $(dirname $0)
. sphynx_common.sh

pushd $GOPATH/src/$GO_PKG
go fmt $GO_PKG/lynxkite-sphynx
go get -v $GO_PKG/lynxkite-sphynx
popd

mkdir -p ../stage/sphynx/go/bin/
rm ../stage/sphynx/go/bin/lynxkite-sphynx || true
cp go/bin/lynxkite-sphynx ../stage/sphynx/go/bin/

kill `pidof go/bin/lynxkite-sphynx`
# Sphynx restarts automatically.
