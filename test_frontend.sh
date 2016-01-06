#!/bin/sh -xue

cd `dirname $0`

cd web
bower install --silent --config.interactive=false
npm install --silent
grunt
cd -

tools/check_documentation.sh
tools/e2e_test.sh
