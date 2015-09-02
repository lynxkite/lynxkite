#!/bin/bash -ue

DIR=$(dirname $0)
cd $DIR/..
if [ ! -f "dev_stage.sh" ]; then
  echo "You must run this script from the source tree, not from inside a stage!"
  exit 1
fi

# Compile.
./dev_stage.sh

# Start backend.
KITE_SITE_CONFIG_OVERRIDES=$(tools/clean_kite_overrides.sh kite_e2e_test/) \
  stage/bin/biggraph interactive &
KITE_PID=$!

cd web
# Make sure the webdriver is installed.
node node_modules/protractor/bin/webdriver-manager update
# Run test against backend.
grunt test_e2e

# Kill backend.
kill $!
