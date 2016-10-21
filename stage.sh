#!/bin/bash -xue

cd $(dirname $0)
pushd web
bower install --silent --config.interactive=false
npm install --silent
gulp
popd

sbt stage
