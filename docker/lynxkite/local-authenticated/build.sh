#!/bin/bash -xue
# Builds a local LynxKite image with authentication.

cd $(dirname $0)

../local/build.sh
docker build -t lynx/kite_local-authenticated:latest .
