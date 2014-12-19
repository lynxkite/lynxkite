#!/bin/bash

set -x
set -e

exit

pushd web
grunt
popd

rm public || true
ln -s web/dist public

sbt stage
