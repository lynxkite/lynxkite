#!/bin/sh -xe

cd `dirname $0`

mkdir logs || true
sbt test

cd web
bower install --silent --config.interactive=false
npm install --offline
grunt jshint
