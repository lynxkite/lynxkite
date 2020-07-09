#!/bin/sh -xue

cd $(dirname $0)

make backend
tools/install_spark.sh
target/unitversal/stage/bin/lynxkite "$@" interactive
