#!/bin/bash

echo alma1

set -ue

TESTS_BASE_DIR=$HOME/kite_daily_tests

DIR=$(dirname $0)

pushd $DIR/.. > /dev/null
KITE_BASE=`pwd`
popd > /dev/null

RANDOM_SUFFIX=`cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 6 | head -n 1`
TODAY=`date "+%Y%m%d"`

BASE_DIR="${TESTS_BASE_DIR}/${TODAY}_${RANDOM_SUFFIX}"

echo "Running kite daily test. Creating meta, data dirs and config under ${BASE_DIR}"

mkdir -p ${BASE_DIR}

# Prepare a config file.
CONFIG_FILE=${BASE_DIR}/kiterc

cat > ${CONFIG_FILE} <<EOF
# !!!Warning!!! Some values are overriden at the end of the file.

`cat ${KITE_BASE}/conf/kiterc_template`

# Override settings created by daily_test.sh
# These will reset some values above. Feel free to edit as necessary.
export KITE_META_DIR=${BASE_DIR}/meta
export KITE_DATA_DIR=file:${BASE_DIR}/data
export EXECUTOR_MEMORY=2g
export NUM_CORES_PER_EXECUTOR=4
export KITE_MASTER_MEMORY_MB=2000
EOF

cd $KITE_BASE

KITE_SITE_CONFIG=${CONFIG_FILE} ./run_batch.sh kitescripts/dailytest
