#!/bin/bash -xue

cd $(dirname $0)

make backend
tools/install_spark.sh
KITE_SITE_CONFIG=${KITE_SITE_CONFIG:-$HOME/.kiterc}

if [ -f ${KITE_SITE_CONFIG} ]; then
  >&2 echo "Loading configuration from: ${KITE_SITE_CONFIG}"
  source ${KITE_SITE_CONFIG}
else
  >&2 echo "Warning, no LynxKite Site Config found at: ${KITE_SITE_CONFIG}"
  >&2 echo "Default location is $HOME/.kiterc, but you can override via the environment variable:"
  >&2 echo "KITE_SITE_CONFIG"
fi

sphynx/go/bin/server &
SPHYNX_PID=$!
function kill_sphynx {
  kill -9 $SPHYNX_PID
}
trap kill_sphynx EXIT ERR

stage/bin/biggraph "$@" interactive
