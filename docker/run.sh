#!/bin/bash
# This is a wrapper for lynxkite/bin/lynxkite. Should be run inside the Docker container!

rm -f root/kite/pid root/kite/sphynx.pid

# Override kiterc settings.
export KITE_HTTP_PORT=2200
export KITE_HTTP_ADDRESS=0.0.0.0
export KITE_DATA_DIR=file:/data
export ORDERED_SPHYNX_DATA_DIR=/data/sphynx/ordered
export UNORDERED_SPHYNX_DATA_DIR=/data/sphynx/unordered
export KITE_MASTER_MEMORY_MB=$KITE_MASTER_MEMORY_MB
export SPHYNX_CACHED_ENTITIES_MAX_MEM_MB=$SPHYNX_CACHED_ENTITIES_MAX_MEM_MB
export SPHYNX_HOST=localhost
export SPHYNX_PORT=50051
export KITE_META_DIR=/metadata
export KITE_EXTRA_JARS=${KITE_EXTRA_JARS:-/extra_jars/*}
touch /prefix_definitions.txt
export KITE_PREFIX_DEFINITIONS=/prefix_definitions.txt
export KITE_ALLOW_PYTHON=${KITE_ALLOW_PYTHON:-yes}
export KITE_ALLOW_R=${KITE_ALLOW_R:-yes}
export KITE_ALLOW_NON_PREFIXED_PATHS=true

exec spark-submit /lynxkite.jar
