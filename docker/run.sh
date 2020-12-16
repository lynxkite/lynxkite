#!/bin/bash
# This is a wrapper for lynxkite/bin/lynxkite. Should be run inside the Docker container!

rm -f root/kite.pid root/sphynx.pid

# Override kiterc settings.
export KITE_HTTP_PORT=${KITE_HTTP_PORT:-2200}
export KITE_HTTP_ADDRESS=${KITE_HTTP_ADDRESS:-0.0.0.0}
export KITE_DATA_DIR=${KITE_DATA_DIR:-file:/data}
export KITE_MASTER_MEMORY_MB=$KITE_MASTER_MEMORY_MB
export SPHYNX_CACHED_ENTITIES_MAX_MEM_MB=$SPHYNX_CACHED_ENTITIES_MAX_MEM_MB
export NUM_CORES_PER_EXECUTOR=${NUM_CORES_PER_EXECUTOR:-'*'}
export SPARK_HOME=/spark
export KITE_META_DIR=${KITE_META_DIR:-/metadata}
export KITE_EXTRA_JARS=${KITE_EXTRA_JARS:-/extra_jars/*}
export KITE_PREFIX_DEFINITIONS=${KITE_PREFIX_DEFINITIONS:-/prefix_definitions.txt}
export KITE_INTERNAL_WATCHDOG_TIMEOUT_SECONDS=${KITE_INTERNAL_WATCHDOG_TIMEOUT_SECONDS:-1200}
export KITE_ALLOW_PYTHON=${KITE_ALLOW_PYTHON:-yes}

export KITE_SITE_CONFIG=${KITE_SITE_CONFIG:-/lynxkite/conf/kiterc_template}

exec lynxkite/bin/lynxkite -Dlogger.resource=logger-docker.xml "$@"
