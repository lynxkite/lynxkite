#!/bin/sh -xue

cd $(dirname $0)

make backend
tools/install_spark.sh
sphynx/go/bin/server &
stage/bin/biggraph "$@" interactive
