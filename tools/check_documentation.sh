#!/bin/bash -ue
# Make sure all operations are documented.
export LC_ALL=C
cat 'web/.tmp/help.html' | sed -n 's/.*h4 id="\([^"]*\)".*/\1/p' | sort > 'web/.tmp/help-links'
sbt --error 'run-main com.lynxanalytics.biggraph.HelpInventory web/.tmp/help-inventory'
MISSING=$(diff -u 'web/.tmp/help-inventory' 'web/.tmp/help-links' | sed -n 's/^-\(\w\)/  \1/p')
if [ "$MISSING" != "" ]; then
  >&2 echo '[ERROR] Documentation is missing for:'
  >&2 echo "$MISSING"
  exit 1
else
  echo 'Documentation verified.'
fi
