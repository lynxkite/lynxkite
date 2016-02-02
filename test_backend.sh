#!/bin/sh -xue

cd `dirname $0`

mkdir logs || true
rm -f logs/* || true
sbt test
! cat logs/test-* | grep "future failed" -A1
