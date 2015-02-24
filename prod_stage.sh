#!/bin/bash

set -x
set -e

pushd web
npm install
bower install
grunt
popd

rm public || true
ln -s web/dist public

sbt stage

rm public || true
