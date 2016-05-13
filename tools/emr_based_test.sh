#!/bin/bash

# Brings up a small EMR cluster and runs tests on it.
# Usage:
#
# emr_based_test.sh frontend  #  Run e2e frontend tests.
#
# emr_based_test.sh bigdata test_pattern param:value
#   Run big data tests specified by the pattern kitescripts/big_data_tests/test_pattern.groovy
#   with a given parameter.
#
#   Example:
#   emr_based_test.sh backend 'big_data_tests/*' testDataSet:fake_westeros_100k
#   This will run all groovy files in kitescripts/perf/*.groovy and all these
#   groovy files will receive the testDataSet:fake_westeros_100k parameter.

source "$(dirname $0)/biggraph_common.sh"
set -x

cd "$(dirname $0)/.."

MODE=${1}
shift

CLUSTER_NAME=${CLUSTER_NAME:-${USER}-test-cluster}
EMR_TEST_SPEC="/tmp/${CLUSTER_NAME}.emr_test_spec"
NUM_INSTANCES=${NUM_INSTANCES:-3}

if [[ ! $NUM_INSTANCES =~ ^[1-9][0-9]*$ ]]; then
  echo "Variable NUM_INSTANCES=$NUM_INSTANCES. This is not a valid instance quantity."
  exit 1
fi

if [[ $NUM_INSTANCES -gt 20 ]]; then
    read -p "NUM_INSTANCES is rather great: $NUM_INSTANCES. Are you sure you want to run this many instances? [Y/n] " answer
    case ${answer:0:1} in
        y|Y|'' )
            ;;
        * )
            exit 1
            ;;
    esac
fi


./stage.sh

cp stage/tools/emr_spec_template ${EMR_TEST_SPEC}
cat >>${EMR_TEST_SPEC} <<EOF

# Override values for the test setup:
CLUSTER_NAME=${CLUSTER_NAME}
NUM_INSTANCES=${NUM_INSTANCES}
S3_DATAREPO=""
KITE_INSTANCE_BASE_NAME=testemr
EOF

CLUSTERID=$(stage/tools/emr.sh clusterid ${EMR_TEST_SPEC})
if [ -n "$CLUSTERID" ]; then
  echo "Reusing already running cluster instance ${CLUSTERID}."
  stage/tools/emr.sh reset-yes ${EMR_TEST_SPEC}
else
  echo "Starting new cluster instance."
  stage/tools/emr.sh start ${EMR_TEST_SPEC}
fi


case $MODE in
  backend )
    stage/tools/emr.sh deploy-kite ${EMR_TEST_SPEC}
    # The next lines are just for invoking:
    # big_data_test_runner.py $1 $2
    # remotely on the master.
    # We need this horror to avoid shell-expansion of the
    # '*' character.
    TMP_SCRIPT=/tmp/${CLUSTER_NAME}_test_script.sh
    SCRIPT_SELECTOR_PATTERN="'$1'"
    shift
    COMMAND_ARGS=( "$@" )
    stage/tools/emr.sh ssh ${EMR_TEST_SPEC} <<ENDSSH
      # Update value of DEV_EXTRA_SPARK_OPTIONS in .kiterc
      sed -i '/^export DEV_EXTRA_SPARK_OPTIONS/d' .kiterc
      echo "export DEV_EXTRA_SPARK_OPTIONS=\"${DEV_EXTRA_SPARK_OPTIONS}\"" >>.kiterc
      biggraphstage/kitescripts/big_data_test_runner.py \
          ${SCRIPT_SELECTOR_PATTERN} ${COMMAND_ARGS[@]}
ENDSSH

    # Upload logs.
    stage/tools/emr.sh uploadLogs ${EMR_TEST_SPEC}
    ;;
  frontend )
    stage/tools/emr.sh kite ${EMR_TEST_SPEC}
    stage/tools/emr.sh connect ${EMR_TEST_SPEC} &
    CONNECTION_PID=$!
    sleep 15

    pushd web
    PORT=4044 gulp test || echo "Frontend tests failed."
    popd

    pkill -TERM -P ${CONNECTION_PID}
    ;;
  * )
    echo "Invalid mode was specified: ${MODE}"
    echo "Usage: $0 backend|frontend"
    exit 1
esac

answer='yes'
# Ask only if STDIN is a terminal.
if [ -t 0 ]; then
  read -p "Test completed. Terminate cluster? [y/N] " answer
fi
case ${answer:0:1} in
  y|Y )
    stage/tools/emr.sh terminate-yes ${EMR_TEST_SPEC}
    ;;
  * )
    echo "Use 'stage/tools/emr.sh ssh ${EMR_TEST_SPEC}' to log in to master."
    echo "Please don't forget to shut down the cluster."
    ;;
esac

