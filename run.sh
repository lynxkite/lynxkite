#!/bin/sh -xue

cd $(dirname $0)

make backend
tools/install_spark.sh
target/universal/stage/bin/lynxkite "$@" interactive
