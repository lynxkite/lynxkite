#!/bin/bash -xue
# Builds the LynxKite single-machine image.

cd $(dirname $0)

VERSION=3.2.1
../../../k8s/cloud-lk/singleuser/get_kite_release.sh $VERSION
rm -rf stage
cp -R ../../../k8s/cloud-lk/singleuser/kite_releases/kite_$VERSION ./stage
docker build -t lynx/kite_local:latest .
