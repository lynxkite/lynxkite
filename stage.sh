#!/bin/bash

set -x
set -e

pushd web
bower install --silent --config.interactive=false
npm install --silent
gulp
popd

sbt stage
