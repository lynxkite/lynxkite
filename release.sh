#!/bin/bash -xue
# Rough automation for rolling and pushing a new release.

cd $(dirname $0)
export VERSION=$1
make
mkdir -p docker/archive
cp target/scala-2.12/lynxkite-$VERSION.jar docker/archive/
cd docker
cp archive/lynxkite-$VERSION.jar lynxkite.jar
cp ../tools/runtime-env.yml lynxkite-env.yml
docker build . -t lynxkite/lynxkite:latest
docker build . -t lynxkite/lynxkite:$VERSION
cp ../tools/runtime-env-cuda.yml lynxkite-env.yml
docker build . --build-arg KITE_ENABLE_CUDA=yes -t lynxkite/lynxkite:latest-cuda
docker build . --build-arg KITE_ENABLE_CUDA=yes -t lynxkite/lynxkite:$VERSION-cuda
docker push lynxkite/lynxkite:$VERSION
docker push lynxkite/lynxkite:$VERSION-cuda
docker push lynxkite/lynxkite:latest
docker push lynxkite/lynxkite:latest-cuda
