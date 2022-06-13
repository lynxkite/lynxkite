#!/bin/bash -xue
# Rough automation for rolling and pushing a new release.

VERSION=$1
make
rm -rf target/universal/stage/logs
echo LynxKite $VERSION > target/universal/stage/version
rm -rf docker/lynxkite-$VERSION*
cp -R target/universal/stage docker/lynxkite-$VERSION
cd docker
rm -rf stage
cp -R lynxkite-$VERSION stage
tar czvf lynxkite-$VERSION.tgz lynxkite-$VERSION
docker build . -t lynxkite/lynxkite:latest
docker build . -t lynxkite/lynxkite:$VERSION
docker build . --build-arg KITE_ENABLE_CUDA=yes -t lynxkite/lynxkite:latest-cuda
docker build . --build-arg KITE_ENABLE_CUDA=yes -t lynxkite/lynxkite:$VERSION-cuda
docker push lynxkite/lynxkite:$VERSION
docker push lynxkite/lynxkite:$VERSION-cuda
docker push lynxkite/lynxkite:latest
docker push lynxkite/lynxkite:latest-cuda
