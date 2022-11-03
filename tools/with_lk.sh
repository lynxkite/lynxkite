#!/bin/bash -xue
# Starts up a LynxKite instance which requires authentication on a random port
# (exported as $HTTPS_PORT/$HTTP_PORT), runs a command, then shuts down LynxKite.

# Make sure Spark is installed.
$(dirname $0)/install_spark.sh

# Create config.
TMP=$(mktemp -d)
KITE_USERS_FILE=$(mktemp)
cat > "$KITE_USERS_FILE" <<EOF
[ {
  "email" : "admin",
  "password" : "adminpw",
  "hash" : "\$2a\$10\$vSHwz.9cHTN5xuEZHMXCmOS45ijJJN2PjqWKIvwgIWQ.RRpmevLRG",
  "isAdmin" : true
}, {
  "email" : "user",
  "password" : "userpw",
  "hash" : "\$2a\$10\$wp846rR04Y/v0TlofIeJ2uPNbC6036Qpl0.AdJeRFP9mDKfR/gWLW",
  "isAdmin" : false
} ]
EOF
KITE_DEPLOYMENT_CONFIG_DIR="$(dirname $0)/../test"
KITE_DEPLOYMENT_CONFIG_DIR=$(realpath "${KITE_DEPLOYMENT_CONFIG_DIR}")
export HTTP_PORT=$[ 9100 + RANDOM % 100 ]
export HTTPS_PORT=$[ 9200 + RANDOM % 100 ]
export SPHYNX_PORT=$[ 9300 + RANDOM % 100 ]
SPHYNX_PID_FILE=${TMP}/sphynx_pid

export SPARK_VERSION=`cat conf/SPARK_VERSION`
. conf/kiterc_template
export KITE_META_DIR="$TMP/meta"
export KITE_DATA_DIR="file:$TMP/data"
export ORDERED_SPHYNX_DATA_DIR=$TMP/ordered_sphynx_data
export UNORDERED_SPHYNX_DATA_DIR=$TMP/unordered_sphynx_data
export SPHYNX_PORT=$SPHYNX_PORT
export KITE_HTTP_PORT=$HTTP_PORT
export KITE_HTTPS_PORT=$HTTPS_PORT
export KITE_APPLICATION_SECRET='<random>'
export KITE_USERS_FILE=$KITE_USERS_FILE
export SPHYNX_PID_FILE=$SPHYNX_PID_FILE
export KITE_HTTPS_KEYSTORE=${KITE_DEPLOYMENT_CONFIG_DIR}/localhost.self-signed.cert
export KITE_HTTPS_KEYSTORE_PWD=keystore-password
export KITE_DOMAINS=sphynx,scala,spark

# Start backend.
$SPARK_HOME/bin/spark-submit \
  --conf "spark.driver.extraJavaOptions=-Dhttp.port=$KITE_HTTP_PORT -Dhttps.port=$KITE_HTTPS_PORT -Dplay.http.secret.key=SECRET-TEST-TEST-TEST-TEST" \
  target/scala-2.12/lynxkite-0.1-SNAPSHOT.jar &
KITE_PID=$!
function kill_backend {
  echo "Shutting down server on port $HTTP_PORT"
  kill $KITE_PID || true
  while kill -0 $KITE_PID 2> /dev/null; do sleep 1; done
  rm -f "$KITE_USERS_FILE"
  rm -rf "$TMP"
}
trap kill_backend EXIT ERR
$(dirname $0)/wait_for_port.sh $SPHYNX_PORT
echo "Sphynx running on port $SPHYNX_PORT"
$(dirname $0)/wait_for_port.sh $HTTP_PORT
$(dirname $0)/wait_for_port.sh $HTTPS_PORT
echo "Kite running on port $HTTP_PORT (http) and port $HTTPS_PORT (https)"

# Execute command.
"$@"
