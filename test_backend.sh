#!/bin/bash -xue

cd `dirname $0`

WITH_SPHYNX='false'
INTERACTIVE='false'
while getopts 'si' flag; do
  case "${flag}" in
    s) WITH_SPHYNX='true' ;;
    i) INTERACTIVE='true' ;;
  esac
done
export WITH_SPHYNX=$WITH_SPHYNX

if $WITH_SPHYNX; then
  # Start Sphynx.
  TMP=$(mktemp -d)
  SPHYNX_PID_FILE=${TMP}/sphynx_pid
  export SPHYNX_PORT=$[ 9400 + RANDOM % 100 ]
  export SPHYNX_CERT_DIR=$TMP/sphynx_cert
  export ORDERED_SPHYNX_DATA_DIR=$TMP/ordered_sphynx_data
  export UNORDERED_SPHYNX_DATA_DIR=$TMP/unordered_sphynx_data
  export KITE_ALLOW_PYTHON=yes
  if [ -f $SPHYNX_PID_FILE ]; then
    kill `cat $SPHYNX_PID_FILE` || true
  fi
  mkdir -p ${SPHYNX_CERT_DIR}
  openssl req -x509 -sha256 -newkey rsa:4096 \
  -keyout "${SPHYNX_CERT_DIR}/private-key.pem" \
  -out "${SPHYNX_CERT_DIR}/cert.pem" -days 365 -nodes \
  -subj "/C=/ST=/L=/O=Lynx Analytics/OU=Org/CN=localhost"
  cd sphynx
  LD_LIBRARY_PATH=.build .build/lynxkite-sphynx -keydir=$SPHYNX_CERT_DIR &
  cd -
  $(dirname $0)/tools/wait_for_port.sh $SPHYNX_PORT
  echo "Sphynx running on port $SPHYNX_PORT"
  echo $! > $SPHYNX_PID_FILE

  function kill_sphynx {
    echo "Shutting down Sphynx."
    SPHYNX_PID=`cat ${SPHYNX_PID_FILE}`
    kill $SPHYNX_PID
    while kill -0 $SPHYNX_PID 2> /dev/null; do sleep 1; done
    rm -rf "$TMP"
  }
  trap kill_sphynx EXIT ERR
fi

mkdir -p logs
rm -f logs/test-*
if $INTERACTIVE; then
  sbt
else
  if $WITH_SPHYNX; then
    sbt "test-only -- -l SparkOnly"
  else
    sbt "test-only -- -l SphynxOnly"
  fi
fi
# We'll check if the logfile contains 'future failed' lines; these
# indicate errors that the test framework cannot catch. In case such
# lines occur, we rename the file logs/test-xxxxxx to logs/failed-xxxxxx
# so that the next invocation of test_backend.sh will not delete it.
thelogfile=logs/test-*
echo logfile: $thelogfile
if grep "future failed" -A1 logs/test-*; then
  failedlog=`echo $thelogfile | sed s:logs/test:logs/failed:`
  mv $thelogfile $failedlog
  exit 1
fi
