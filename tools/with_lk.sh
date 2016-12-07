#!/bin/bash -xue
# Starts up a LynxKite instance on a random port (exported as $PORT), runs a command, then shuts
# down LynxKite.

COMMAND=$@

if [ ! -f "$(dirname $0)/../stage.sh" ]; then
  echo "You must run this script from the source tree, not from inside a stage!"
  exit 1
fi

# Make sure Spark is installed.
$(dirname $0)/install_spark.sh

# Create config.
TMP=$(mktemp -d)
export PORT=$[ 9100 + RANDOM % 100 ]
PID_FILE=${TMP}/pid
cat > "$TMP/overrides"  <<EOF
export KITE_META_DIR="$TMP/meta"
export KITE_DATA_DIR="file:$TMP/data"
export KITE_HTTP_PORT=$PORT
export KITE_PID_FILE=$PID_FILE
EOF

# Start backend.
KITE_SITE_CONFIG="conf/kiterc_template" \
KITE_SITE_CONFIG_OVERRIDES="$TMP/overrides" stage/bin/biggraph start
KITE_PID=`cat ${PID_FILE}`
function kill_backend {
  echo "Shutting down server on port $PORT"
  kill $KITE_PID
  while kill -0 $KITE_PID 2> /dev/null; do sleep 1; done
  rm -rf "$TMP"
}
trap kill_backend EXIT ERR
$(dirname $0)/wait_for_port.sh $PORT
echo "Kite running on port: $PORT"

# Execute command.
$COMMAND
