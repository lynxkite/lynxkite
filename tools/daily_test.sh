#!/bin/bash -ue

DIR=$(dirname $0)
cd $DIR/..
if [ ! -f "bin/biggraph" ]; then
  echo "You must run this script from inside a stage, not from the source tree!"
  exit 1
fi

KITE_SITE_CONFIG_OVERRIDES=$(tools/clean_kite_overrides.sh kite_daily_test/) \
  bin/biggraph batch `pwd`/kitescripts/dailytest.groovy
