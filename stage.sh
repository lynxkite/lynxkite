#!/bin/bash

set -x
set -e

pushd web
npm install
bower install
gulp
popd

sbt stage
