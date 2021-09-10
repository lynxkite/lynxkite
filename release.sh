#!/bin/bash -xue
# Rough automation for rolling and pushing a new release.

VERSION=$1
make
git tag $VERSION
git push --tags
rm -rf target/universal/stage/logs
echo LynxKite $VERSION > target/universal/stage/version
rm -rf docker/lynxkite-$VERSION*
cp -R target/universal/stage docker/lynxkite-$VERSION
cd docker
rm -rf stage
cp -R lynxkite-$VERSION stage
docker build . -t lynxkite/lynxkite:latest
docker build . -t lynxkite/lynxkite:$VERSION
tar czvf lynxkite-$VERSION.tgz lynxkite-$VERSION
aws s3 cp lynxkite-$VERSION.tgz s3://kite-releases/public/
docker push lynxkite/lynxkite:$VERSION
docker push lynxkite/lynxkite:latest
