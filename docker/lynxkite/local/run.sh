#!/bin/bash
# This is a wrapper for lynxkite/bin/lynxkite. Should be run inside the Docker container!

rm -f root/kite.pid root/sphynx.pid

# Configure kiterc settings.
cat > /kiterc_overrides <<EOF
export KITE_INSTANCE="$KITE_INSTANCE"
export KITE_HTTP_PORT=2200
export KITE_HTTP_ADDRESS=0.0.0.0
export KITE_DATA_DIR=file:/data
export KITE_MASTER_MEMORY_MB=$KITE_MASTER_MEMORY_MB
export SPHYNX_CACHED_ENTITIES_MAX_MEM_MB=$SPHYNX_CACHED_ENTITIES_MAX_MEM_MB
export NUM_CORES_PER_EXECUTOR='*'
export SPARK_HOME=/spark
export KITE_META_DIR=/metadata
export KITE_EXTRA_JARS=/extra_jars/*
export KITE_PREFIX_DEFINITIONS=/prefix_definitions.txt
export KITE_INTERNAL_WATCHDOG_TIMEOUT_SECONDS=1200
export KITE_ALLOW_PYTHON=yes
EOF

export KITE_SITE_CONFIG=/lynxkite/conf/kiterc_template
export KITE_SITE_CONFIG_OVERRIDES=/kiterc_overrides

exec lynxkite/bin/lynxkite -Dlogger.resource=logger-docker.xml "$@"
