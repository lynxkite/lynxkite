#!/bin/bash -xue

cd `dirname $0`

mkdir -p logs
rm -f logs/test-*
sbt test

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
