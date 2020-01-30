#!/bin/bash -xue
# Compiles and restarts the sphynx server, quickly.
# This does not recompile protobuf and does not download
# the newest package versions

export SPARK_VERSION=something
KITE_SITE_CONFIG=${KITE_SITE_CONFIG:-$HOME/.kiterc}

. $KITE_SITE_CONFIG

if [ -f $SPHYNX_PID_FILE ]; then
    kill `cat $SPHYNX_PID_FILE` || true
fi


cd $(dirname $0)
. sphynx_common.sh

pushd $GOPATH/src/$GO_PKG
go fmt $GO_PKG/server
go get -v $GO_PKG/server
popd
mkdir -p ../stage/sphynx/go/bin/
cp go/bin/server ../stage/sphynx/go/bin/
../stage/sphynx/go/bin/server -keydir=$SPHYNX_CERT_DIR

