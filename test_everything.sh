#!/bin/sh -xe

cd `dirname $0`

mkdir logs || true
sbt test
sbt --error 'run-main com.lynxanalytics.biggraph.HelpInventory' > web/.tmp/help-inventory

cd web
bower install --silent --config.interactive=false
npm install --silent
npm test
