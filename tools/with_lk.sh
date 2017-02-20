#!/bin/bash -xue
# Starts up a LynxKite instance which requires authentication on a random port
# (exported as $HTTPS_PORT/$HTTP_PORT), runs a command, then shuts down LynxKite.

COMMAND=$@

if [ ! -f "$(dirname $0)/../stage.sh" ]; then
  echo "You must run this script from the source tree, not from inside a stage!"
  exit 1
fi

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
KITE_DEPLOYMENT_CONFIG_DIR="test"
export HTTP_PORT=$[ 9100 + RANDOM % 100 ]
export HTTPS_PORT=$[ 9200 + RANDOM % 100 ]
PID_FILE=${TMP}/pid
cat > "$TMP/overrides"  <<EOF
export KITE_META_DIR="$TMP/meta"
export KITE_DATA_DIR="file:$TMP/data"
export KITE_HTTP_PORT=$HTTP_PORT
export KITE_HTTPS_PORT=$HTTPS_PORT
export KITE_APPLICATION_SECRET='<random>'
export KITE_USERS_FILE=$KITE_USERS_FILE
export KITE_PID_FILE=$PID_FILE
export KITE_HTTPS_KEYSTORE=${KITE_DEPLOYMENT_CONFIG_DIR}/localhost.self-signed.cert
export KITE_HTTPS_KEYSTORE_PWD=keystore-password
EOF

# Start backend.
KITE_SITE_CONFIG="conf/kiterc_template" \
KITE_SITE_CONFIG_OVERRIDES="$TMP/overrides" stage/bin/biggraph start
KITE_PID=`cat ${PID_FILE}`
function kill_backend {
  echo "Shutting down server on port $HTTP_PORT"
  kill $KITE_PID
  while kill -0 $KITE_PID 2> /dev/null; do sleep 1; done
  rm -rf "$TMP"
  rm -f "$KITE_USERS_FILE"
}
trap kill_backend EXIT ERR
$(dirname $0)/wait_for_port.sh $HTTP_PORT
echo "Kite running on port $HTTP_PORT (http) and port $HTTPS_PORT (https)"

# Execute command.
$COMMAND
