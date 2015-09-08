#!/bin/sh -xe

cd `dirname $0`

mkdir logs || true
sbt -Dsbt.log.noformat=true test

cd web
bower install --silent --config.interactive=false
npm install --silent
npm test

cd -
tools/check_documentation.sh
tools/e2e_test.sh
