#!/bin/bash -xue

cd $(dirname $0)

make backend
tools/install_spark.sh
sphynx/go/bin/server &
SPHYNX_PID=$!
function kill_sphynx {
  kill -9 $SPHYNX_PID
}
trap kill_sphynx EXIT ERR

stage/bin/biggraph "$@" interactive
