#!/bin/bash
#
# Simple script to do an independent Kite test run on EC2 for benchmarking purposes.
# We will run Kite using a clean meta and data directory.
#
# The user is expected to export environment variables to controll the details of the run. In
# additional to optionally changing Kite configuration via standard env variables, one needs to
# at least set up the following:
#   NUM_CORES_PER_EXECUTOR,EXECUTOR_MEMORY,GLOB
#
# GLOB is a glob specifying the csv files we should import as part of the test.

RANDOM_ID=`cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 8 | head -n 1`

set -ueo pipefail

DIR=$(dirname $0)
pushd $DIR/.. > /dev/null
KITE_BASE=`pwd`
popd > /dev/null

HDFS_DATA="hdfs://$(curl http://instance-data.ec2.internal/latest/meta-data/public-hostname):9000/data"
DATA_DIR_BASE=${DATA_DIR_BASE:-${HDFS_DATA}}
DATA_DIR="${DATA_DIR_BASE}/${RANDOM_ID}"
mkdir -p "${HOME}/one-run-meta"
META_DIR="${HOME}/one-run-meta/${RANDOM_ID}"

mkdir -p "${HOME}/one-run-confs"
OVERRIDES_FILE="${HOME}/one-run-confs/${RANDOM_ID}"

cat > ${OVERRIDES_FILE} <<EOF
export KITE_META_DIR="${META_DIR}"
export KITE_EPHEMERAL_DATA_DIR=""
export KITE_DATA_DIR="${DATA_DIR}"
export NUM_CORES_PER_EXECUTOR=${NUM_CORES_PER_EXECUTOR}
export EXECUTOR_MEMORY=${EXECUTOR_MEMORY}
EOF

KITE_SITE_CONFIG_OVERRIDES=${OVERRIDES_FILE} \
  ${KITE_BASE}/bin/biggraph batch ${KITE_BASE}/kitescripts/importtest glob:${GLOB}
