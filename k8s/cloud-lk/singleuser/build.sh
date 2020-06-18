#!/bin/bash -xue
# Builds the required LynxKite version into a Docker image.
# The version number is hardwired.

VERSION=4.0.0-preview

cd $(dirname $0)
rm -rf build
mkdir -p build/stage
rsync -avL ../../../python/remote_api build/python/
rsync -avL ../../../python/documentation build/python/

cp ../../../tools/upload_workspace.py scripts
cp ../../../tools/wait_for_port.sh scripts

cp -a lynxkite-${VERSION}/* build/stage

docker build -t gcr.io/external-lynxkite/cloud-lk-notebook:v1 .
