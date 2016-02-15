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

grep "future failed" -A1 logs/test-*
if [ "$?" == "0" ]; then
    thelogfile=logs/test-*
    failedlog=`echo $thelogfile | sed s:logs/test:logs/failed:`
    mv $thelogfile $failedlog
    exit 1
else
    exit 0
fi
