#!/bin/bash

# Runs synthetic Spark tests inside LynxKite.

if [[ $# -eq 0 ]]; then
  # Take default args.
  MODE="remote"
  DATA_SIZE="2000000000"
  NUM_PARTITIONS="1000"
  TEST_SELECTOR="all.list"
elif [[ $# -lt 3 || ( "$1" != "local" && "$1" != "remote" ) ]]; then
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
else
  # Take command line args.
  MODE=$1
  DATA_SIZE=$2
  NUM_PARTITIONS=$3
  TEST_SELECTOR=${4:-all.list}
fi

set -xueo pipefail
trap "echo $0 has failed" ERR

DIR_SUFFIX=${DEV_EXTRA_SPARK_OPTIONS:-}
DIR_SUFFIX=${DIR_SUFFIX//[[:blank:]]/}

RESULTS_DIR="$(dirname $0)/kitescripts/spark_tests/results/${MODE}_${DATA_SIZE}_${NUM_PARTITIONS}$DIR_SUFFIX"

case $MODE in
  "local" )
    mkdir -p ${RESULTS_DIR}
    $(dirname $0)/stage.sh
    TESTS_TO_RUN=$(
      $(dirname $0)/stage/kitescripts/big_data_test_scheduler.py \
          --remote_lynxkite_path=$(dirname $0)/stage/bin/biggraph \
          --remote_test_dir=$(dirname $0)/kitescripts/spark_tests \
          --local_test_dir=$(dirname $0)/kitescripts/spark_tests \
          --test_selector="${TEST_SELECTOR}" \
          --lynxkite_arg="dataSize:${DATA_SIZE}" \
          --lynxkite_arg="numPartitions:${NUM_PARTITIONS}" \
          --remote_output_dir=${RESULTS_DIR}
        )
    while read -r LINE; do
      eval $LINE
    done <<< "$TESTS_TO_RUN"
    ;;
  "remote" )
    EMR_RESULTS_DIR=$RESULTS_DIR \
      $(dirname $0)/tools/emr_based_test.sh backend \
        --remote_test_dir=/home/hadoop/biggraphstage/kitescripts/spark_tests \
        --local_test_dir=$(dirname $0)/kitescripts/spark_tests \
        --test_selector="${TEST_SELECTOR}" \
        --lynxkite_arg="dataSize:${DATA_SIZE}" \
        --lynxkite_arg="numPartitions:${NUM_PARTITIONS}"
    ;;
esac

grep --no-filename 'FINISHED SCRIPT\|STAGE DONE' \
    ${RESULTS_DIR}/*.out.txt >${RESULTS_DIR}/filtered_log.txt

