#!/bin/bash -ue

DIR=$(dirname $0)
cd $DIR/..
if [ ! -f "dev_stage.sh" ]; then
  echo "You must run this script from the source tree, not from inside a stage!"
  exit 1
fi

# Compile.
./dev_stage.sh

# Create config.
TMP=$(mktemp -d)
PORT=$[ 9100 + RANDOM % 100 ]
cat > "$TMP/overrides"  <<EOF
export KITE_META_DIR="$TMP/meta"
export KITE_DATA_DIR="file:$TMP/data"
export KITE_HTTP_PORT=$PORT
EOF

# Start backend.
KITE_SITE_CONFIG="conf/kiterc_template" \
KITE_SITE_CONFIG_OVERRIDES="$TMP/overrides" \
  stage/bin/biggraph interactive &
KITE_PID=$!

cd web
# Make sure the webdriver is installed.
node node_modules/protractor/bin/webdriver-manager update
# Run test against backend.
grunt test_e2e --port=$PORT || true # Kill backend even if test fails.

# Kill backend.
kill $!
rm -rf "$TMP"
