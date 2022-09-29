#!/bin/bash -xue
# Make sure all operations are documented.
cd $(dirname $0)/..
export LC_ALL=C
cat 'web/app/help/index.html' | sed -n 's/.*h4 id="\([^"]*\)".*/\1/p' | sort > 'web/.tmp/help-links'
sbt --error 'runMain com.lynxanalytics.lynxkite.HelpInventory web/.tmp/help-inventory'
MISSING=$(diff -u 'web/.tmp/help-inventory' 'web/.tmp/help-links' | sed -n 's/^-\(\w\)/  \1/p')
if [ "$MISSING" != "" ]; then
  >&2 echo '[ERROR] Documentation is missing for:'
  >&2 echo "$MISSING"
  exit 1
else
  echo 'Documentation verified.'
fi
