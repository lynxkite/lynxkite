#!/bin/bash -xu

cd `dirname $0`
if [ "$?" != "0" ]; then
    exit 1
fi

mkdir logs
rm -f logs/test-*
sbt test
if [ "$?" != "0" ]; then
    exit 1
fi

# We'll check if the logfile contains 'future failed' lines; these
# indicate errors that the test framework cannot catch. In case such
# lines occur, we rename the file logs/test-xxxxxx to logs/failed-xxxxxx
# so that the next invokation of test_backend.sh will not delete it.
thelogfile=logs/test-*
echo logfile: $thelogfile
grep "future failed" -A1 logs/test-*
if [ "$?" == "0" ]; then
    failedlog=`echo $thelogfile | sed s:logs/test:logs/failed:`
    mv $thelogfile $failedlog
    exit 1
else
    exit 0
fi
