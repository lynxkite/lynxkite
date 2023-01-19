#!/bin/bash -xue
# Starts up a LynxKite instance which requires authentication on a random port
# (exported as $HTTPS_PORT/$HTTP_PORT), runs a command, then shuts down LynxKite.

# Create config.
TMP=$(mktemp -d)
REPO=$(dirname $0)/..
REPO=$(realpath $REPO)
. $REPO/conf/kiterc_template
export KITE_USERS_FILE=$(mktemp)
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
export KITE_HTTP_PORT=$[ 9100 + RANDOM % 100 ]
export KITE_HTTPS_PORT=$[ 9200 + RANDOM % 100 ]
export SPHYNX_PORT=$[ 9300 + RANDOM % 100 ]
export SPHYNX_PID_FILE=${TMP}/sphynx_pid
export KITE_META_DIR="$TMP/meta"
export KITE_DATA_DIR="file:$TMP/data"
export ORDERED_SPHYNX_DATA_DIR=$TMP/ordered_sphynx_data
export UNORDERED_SPHYNX_DATA_DIR=$TMP/unordered_sphynx_data
export KITE_HTTPS_KEYSTORE=$REPO/test/localhost.self-signed.cert
export KITE_HTTPS_KEYSTORE_PWD=keystore-password
export KITE_DOMAINS=sphynx,scala,spark
export LYNXKITE_ADDRESS=${LYNXKITE_ADDRESS:-http://localhost:$KITE_HTTP_PORT}
export KITE_ALLOW_NON_PREFIXED_PATHS=true

# Start backend.
cd $REPO
spark-submit \
  --conf "spark.driver.extraJavaOptions=-Dplay.http.secret.key=SECRET-TEST-TEST-TEST-TEST -Dhttps.keyStore=$KITE_HTTPS_KEYSTORE -Dhttps.keyStorePassword=$KITE_HTTPS_KEYSTORE_PWD" \
  target/scala-2.12/lynxkite-0.1-SNAPSHOT.jar > /dev/null 2>&1 &
KITE_PID=$!
cd -
function kill_backend {
  echo "Shutting down server on port $KITE_HTTP_PORT"
  kill $KITE_PID || true
  while kill -0 $KITE_PID 2> /dev/null; do sleep 1; done
  rm -f "$KITE_USERS_FILE"
  rm -rf "$TMP"
}
trap kill_backend EXIT ERR
$REPO/tools/wait_for_port.sh $KITE_HTTP_PORT
$REPO/tools/wait_for_port.sh $KITE_HTTPS_PORT
echo "LynxKite running on port $KITE_HTTP_PORT (http) and port $KITE_HTTPS_PORT (https)"

# Execute command.
"$@"
