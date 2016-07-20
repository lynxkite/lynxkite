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


$(dirname $0)/../stage.sh

cp $(dirname $0)/../stage/tools/emr_spec_template ${EMR_TEST_SPEC}
cat >>${EMR_TEST_SPEC} <<EOF

# Override values for the test setup:
CLUSTER_NAME=${CLUSTER_NAME}
NUM_INSTANCES=${NUM_INSTANCES}
S3_DATAREPO=""
KITE_INSTANCE_BASE_NAME=testemr
EOF

EMR_SH=$(dirname $0)/../stage/tools/emr.sh

CLUSTERID=$(${EMR_SH} clusterid ${EMR_TEST_SPEC})
if [ -n "$CLUSTERID" ]; then
  echo "Reusing already running cluster instance ${CLUSTERID}."
  ${EMR_SH} reset-yes ${EMR_TEST_SPEC}
else
  echo "Starting new cluster instance."
  ${EMR_SH} start ${EMR_TEST_SPEC}
fi


case $MODE in
  backend )
    ${EMR_SH} deploy-kite ${EMR_TEST_SPEC}

    COMMAND_ARGS=( "$@" )
    REMOTE_OUTPUT_DIR=/home/hadoop/test_results
    TESTS_TO_RUN=$(./stage/kitescripts/big_data_test_scheduler.py \
        --remote_lynxkite_path=/home/hadoop/biggraphstage/bin/biggraph \
        --remote_output_dir=$REMOTE_OUTPUT_DIR \
        ${COMMAND_ARGS[@]} )
    START_MYSQL=$(echo "$TESTS_TO_RUN" | grep 'mysql' || true)
    if [ -n "$START_MYSQL" ]; then
      if [ -n "$CLUSTERID" ]; then
        MYSQL=$(ENGINE=MySQL ${EMR_SH} rds-get ${EMR_TEST_SPEC})
      else
        MYSQL=$(ENGINE=MySQL ${EMR_SH} rds-up ${EMR_TEST_SPEC})
      fi
    else
      MYSQL=''
    fi
    START_ORACLE=$(echo "$TESTS_TO_RUN" | grep 'oracle' || true)
    if [ -n "$START_ORACLE" ]; then
      if [ -n "$CLUSTERID" ]; then
        ORACLE=$(ENGINE=oracle-se ${EMR_SH} rds-get ${EMR_TEST_SPEC})
      else
        ORACLE=$(ENGINE=oracle-se ${EMR_SH} rds-up ${EMR_TEST_SPEC})
      fi
    else
      ORACLE=''
    fi

    (
      ${EMR_SH} ssh ${EMR_TEST_SPEC} <<ENDSSH
        # We will track completion of the test using this file.
        # This trickery is needed to handle the case nicely when
        # the Internet connection is broken.
        # Possible statuses: not_finished, done
        echo "not_finished" >~/test_status.txt

        # Update value of DEV_EXTRA_SPARK_OPTIONS in .kiterc
        sed -i '/^export DEV_EXTRA_SPARK_OPTIONS/d' .kiterc
        echo "export DEV_EXTRA_SPARK_OPTIONS=\"${DEV_EXTRA_SPARK_OPTIONS:-}\"" >>.kiterc
        # Prepare output dir.
        rm -Rf ${REMOTE_OUTPUT_DIR}
        mkdir -p ${REMOTE_OUTPUT_DIR}
        # Export database addresses.
        export MYSQL=$MYSQL
        export ORACLE=$ORACLE

        # Put tests to run into a script file.
        echo "${TESTS_TO_RUN[@]}" >test_cmds.sh
        # Last command in the script signals completion.
        echo "echo \"done\" >~/test_status.txt" >>test_cmds.sh
        chmod a+x test_cmds.sh

        # Kill running instance (if any).
        pkill -f 'sh \./test_cmds.sh'

        # Use nohup to prevent death when the ssh connection
        # goes away. We also make sure to set status to "done"
        # even in case of a failure so that the below polling
        # loop can exit.
        nohup ./test_cmds.sh >~/test_output.txt \
          || echo "done"> ~/test_status.txt &

        SCRIPT_PID=\$!
        tail -f ~/test_output.txt --pid=\$SCRIPT_PID
    ) || echo "SSH failed but not giving up!"
    echo "SSH connection to cluster is now closed."

    # Keep polling the master whether tests are done.
    STATUS=""
    while true; do
      echo "Polling test status file at master."
      STATUS=$(${EMR_SH} cmd ${EMR_TEST_SPEC} "cat ~/test_status.txt" || echo "ssh_failed")
      echo "status: $STATUS"
      if [[ "$STATUS" == "done" ]]; then
        break
      fi
      sleep 10
    done


    # Process output files.
    if [ -n "${EMR_RESULTS_DIR}" ]; then
      mkdir -p ${EMR_RESULTS_DIR}
      # Download log files for each test:
      ${EMR_SH} download-dir ${EMR_TEST_SPEC} \
          ${REMOTE_OUTPUT_DIR}/ ${EMR_RESULTS_DIR}/

      # Trim output files: removes lines from before
      # STARTING SCRIPT and after FINISHED SCRIPT.
      # For example, this will discard extremely
      # long stack traces generated by Kryo which are printed
      # after the "FINISHED" line.
      for OUTPUT_FILE in ${EMR_RESULTS_DIR}/*.out.txt; do
        mv $OUTPUT_FILE /tmp/emr_based_test.$$.txt
        cat /tmp/emr_based_test.$$.txt | \
          awk '/STARTING SCRIPT/{flag=1}/FINISHED SCRIPT/{print;flag=0}flag' >${OUTPUT_FILE}
      done

      # Create nice summary file:
      cat ${EMR_RESULTS_DIR}/*.out.txt | \
          grep FINISHED | \
          sed 's/^FINISHED SCRIPT \(.*\), took \(.*\) seconds$/\1:\2/' \
            >${EMR_RESULTS_DIR}/summary.txt
    fi

    # Upload logs.
    ${EMR_SH} uploadLogs ${EMR_TEST_SPEC}
    ;;
  frontend )
    ${EMR_SH} kite ${EMR_TEST_SPEC}
    ${EMR_SH} connect ${EMR_TEST_SPEC} &
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
    ${EMR_SH} terminate-yes ${EMR_TEST_SPEC}
    if [ -n "$START_MYSQL" ]; then
      ENGINE=MySQL ${EMR_SH} rds-down ${EMR_TEST_SPEC}
    fi
    if [ -n "$START_ORACLE" ]; then
      ENGINE=oracle-se ${EMR_SH} rds-down ${EMR_TEST_SPEC}
    fi
    ;;
  * )
    echo "Use 'stage/tools/emr.sh ssh ${EMR_TEST_SPEC}' to log in to master."
    echo "Please don't forget to shut down the cluster."
    ;;
esac

