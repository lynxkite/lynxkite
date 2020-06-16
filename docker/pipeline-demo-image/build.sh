#!/bin/bash -xue

cd $(dirname $0)

VERSION=3.2.1
../../k8s/cloud-lk/singleuser/get_kite_release.sh $VERSION
rm -rf build
mkdir build
rsync -avL ../../python/remote_api build/python/
rsync -avL ../../python/automation build/python/
cp -R ../../k8s/cloud-lk/singleuser/kite_releases/kite_$VERSION build/stage
cp ../../tools/wait_for_port.sh build/stage/tools

docker build -t pipeline-demo .
