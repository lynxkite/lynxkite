#!/bin/sh -xue

cd $(dirname $0)

make backend
tools/install_spark.sh
stage/bin/biggraph "$@" interactive
