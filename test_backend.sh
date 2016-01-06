#!/bin/sh -xue

cd `dirname $0`

mkdir logs || true
sbt test
