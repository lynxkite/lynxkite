#!/bin/bash -xue
# Builds the LynxKite single-machine image.

cd $(dirname $0)

VERSION=4.0.1
rm -rf stage
cp -R lynxkite-enterprise-$VERSION ./stage
docker build -t lynx/kite_local:latest .
