#!/bin/bash

set -ueo pipefail
trap 'echo Failed.' ERR

cd $(dirname $0)

CLUSTER_NAME="${USER}-test-cluster"
EMR_TEST_SPEC="/tmp/${CLUSTER_NAME}.emr_test_spec"

./stage.sh

cp stage/tools/emr_spec_template ${EMR_TEST_SPEC}
cat >>${EMR_TEST_SPEC} <<EOF

# Override values for the test setup:
CLUSTER_NAME=${CLUSTER_NAME}
NUM_INSTANCES=3
S3_DATAREPO=""

EOF

CLUSTERID=$(stage/tools/emr.sh clusterid ${EMR_TEST_SPEC})
if [ -n "$CLUSTERID" ]; then
  echo "Reusing already running cluster instance ${CLUSTERID}."
  stage/tools/emr.sh reset-yes ${EMR_TEST_SPEC}
else
  echo "Starting new cluster instance."
  stage/tools/emr.sh start ${EMR_TEST_SPEC}
fi

stage/tools/emr.sh kite ${EMR_TEST_SPEC}
stage/tools/emr.sh batch ${EMR_TEST_SPEC} kitescripts/perf/*.groovy

read -p "Test completed. Terminate cluster? [y/N] " answer
case ${answer:0:1} in
  y|Y )
    stage/tools/emr.sh terminate-yes ${EMR_TEST_SPEC}
    ;;
  * )
    echo "Use 'stage/tools/emr.sh ssh ${EMR_TEST_SPEC}' to log in to master."
    echo "Please don't forget to shut down the cluster."
    ;;
esac

