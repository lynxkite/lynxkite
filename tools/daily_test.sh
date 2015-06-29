#!/bin/bash

set -ue

TESTS_NAME_PREFIX="kite_daily_tests/"

DIR=$(dirname $0)

pushd $DIR/.. > /dev/null
KITE_BASE=`pwd`
popd > /dev/null

if [ ! -f "${KITE_BASE}/bin/biggraph" ]; then
  echo "You must run this script from inside a stage, not from the source tree!"
  exit 1
fi

RANDOM_SUFFIX=`cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 6 | head -n 1`
TODAY=`date "+%Y%m%d"`

TEST_NAME="${TESTS_NAME_PREFIX}${TODAY}_${RANDOM_SUFFIX}"

echo "Running kite daily test: ${TEST_NAME}"

# Prepare the overrides file.
OVERRIDES_FILE="/tmp/$(basename $TEST_NAME).overrides"

cat > ${OVERRIDES_FILE} <<EOF
export KITE_META_DIR=\${KITE_META_DIR}/${TEST_NAME}
export KITE_DATA_DIR=\${KITE_DATA_DIR}/${TEST_NAME}
EOF

KITE_SITE_CONFIG_OVERRIDES=${OVERRIDES_FILE} \
  ${KITE_BASE}/bin/biggraph batch ${KITE_BASE}/kitescripts/dailytest
