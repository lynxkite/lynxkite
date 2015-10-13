#!/bin/bash

set -x
set -e

pushd web
npm install
bower install
grunt
popd

sbt stage
