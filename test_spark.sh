#!/bin/bash

# Runs synthetic Spark tests inside LynxKite.

if [ $# -ne 2 ] || [ "$1" != "local" ] && [ "$1" != "remote" ]; then

  echo "Usage:"
  echo "  test_spark.sh local|remote data_size partition_size [test_name_pattern]"
  echo
  echo "Runs synthetic Spark tests on EMR or locally. The Spark tests "
  echo "are running inside LynxKite so they are using the same Spark settings."
  echo
  echo "Examples:"
  echo "  test_spark.sh local 100000 100"
  echo "  test_spark.sh remote 1000000 100"
  echo "  DEV_EXTRA_SPARK_OPTIONS=\"-conf spark.locality.wait=999m --conf spark.rdd.compress=true \""
  echo
  echo "This will run the tests in TestSpark.scala."
  exit 1
fi

set -xueo pipefail
trap "echo $0 has failed" ERR

MODE=$1
DATA_SIZE=$2
NUM_PARTITIONS=$3
TEST_NAME_PATTERN=${4:-*}

cd $(dirname $0)

if [ "$MODE" = "local" ]; then
  ./stage.sh
fi

runTests() {
  case $MODE in
    "local" )
      ./stage/kitescripts/big_data_test_runner.py \
          "spark_tests/$TEST_NAME_PATTERN" \
          dataSize:$DATA_SIZE numPartitions:$NUM_PARTITIONS
      ;;
    "remote" )
      tools/emr_based_test.sh backend \
          "spark_tests/$TEST_NAME_PATTERN" \
          dataSize:$DATA_SIZE numPartitions:$NUM_PARTITIONS 2>&1
      ;;
  esac

}

RESULTS_DIR="kitescripts/spark_tests/results"
FNAME_BASE="results_$(date +%Y%m%d_%H%M%S)"
NEW_RESULTS_FILE="${RESULTS_DIR}/${FNAME_BASE}.md"
OUTPUT_LOG="${RESULTS_DIR}/${FNAME_BASE}.log"

rm -f ${NEW_RESULTS_FILE}

runTests 2>&1 | tee ${OUTPUT_LOG}

echo "export DEV_EXTRA_SPARK_OPTIONS=\"${DEV_EXTRA_SPARK_OPTIONS}\"" >${NEW_RESULTS_FILE}
echo "$*" >>${NEW_RESULTS_FILE}
grep 'FINISHED SCRIPT\|STAGE DONE' ${OUTPUT_LOG} >>${NEW_RESULTS_FILE}

