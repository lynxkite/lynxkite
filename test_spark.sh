#!/bin/bash

if [ $# -ne 2 ] || [ "$1" != "local" ] && [ "$1" != "remote" ]; then

  echo "Usage:"
  echo "  test_spark.sh start_size num_iterations local|remote [test_name_pattern]"
  echo
  echo "Examples:"
  echo "  test_spark.sh local 100000"
  echo "  test_spark.sh remote 1000000"
  echo
  echo "This will run the tests in TestSpark.scala."
  exit 1
fi

set -xueo pipefail
trap "echo $0 has failed" ERR

MODE=$1
DATA_SIZE=$2
TEST_NAME_PATTERN=${3:-*}

cd $(dirname $0)

if [ "$MODE" = "local" ]; then
  ./stage.sh
fi

runTests() {
  case $MODE in
    "local" )
      ./stage/kitescripts/big_data_test_runner.py "spark_tests/$TEST_NAME_PATTERN" dataSize:$DATA_SIZE
      ;;
    "remote" )
      tools/emr_based_test.sh backend "spark_tests/$TEST_NAME_PATTERN" dataSize:$DATA_SIZE 2>&1
      ;;
  esac

}

RESULTS_DIR="kitescripts/spark_tests/results"
NEW_RESULTS_FILE="${RESULTS_DIR}/${MODE}_${DATA_SIZE}.md"
OUTPUT_LOG="${RESULTS_DIR}/${MODE}_${DATA_SIZE}.log"

rm -f ${NEW_RESULTS_FILE}

runTests 2>&1 | tee ${OUTPUT_LOG}

#grep FINISHED ${OUTPUT_LOG} | \
#  awk '{ n=split($3,s,"/"); printf "%s: %s\n",substr(s[n],0,length(s[n])),$5 }' >>${NEW_RESULTS_FILE}

grep 'FINISHED SCRIPT\|STAGE DONE' ${OUTPUT_LOG} >${NEW_RESULTS_FILE}

