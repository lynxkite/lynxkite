#!/bin/bash -xue
# Builds and uploads the LynxKite single-machine docker image.

source $(dirname $0)/build.sh
docker tag lynx/kite_local:latest lynxkite/lynxkite:${VERSION}
docker tag lynx/kite_local:latest lynxkite/lynxkite:latest
docker login
docker push lynxkite/lynxkite:${VERSION}
docker push lynxkite/lynxkite:latest
