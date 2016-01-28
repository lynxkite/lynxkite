#!/bin/bash

./stage.sh

cd stage/tools

cp emr_spec_template emr_test_spec
cat >>emr_test_spec <<EOF
# Override values for the test setup:
CLUSTER_NAME=${USER}-test-cluster
NUM_INSTANCES=1
S3_DATAREPO=""

EOF

CLUSTERID=$(./emr.sh clusterid emr_test_spec)
if [ -n "$CLUSTERID" ]; then
  echo "Reusing already running cluster instance ${CLUSTERID}."
  ./emr.sh reset-yes emr_test_spec
else
  echo "Starting new cluster instance."
  ./emr.sh start emr_test_spec
fi

./emr.sh kite emr_test_spec
./emr.sh batch emr_test_spec ../kitescripts/jsperf.groovy ../kitescripts/visualization_perf.groovy

read -p "Test completed. Terminate cluster? [y/N] " answer
case ${answer:0:1} in
  y|Y )
    ./emr.sh terminate-yes emr_test_spec
    ;;
  * )
    echo "Use 'cd stage/tools && ./emr.sh ssh emr_test_spec' to log in to master."
    echo "Please don't forget to shut down the cluster."
    ;;
esac

