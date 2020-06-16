#!/bin/bash -xue
# Builds a LynxKite image for YARN with authentication.

cd $(dirname $0)

../yarn/build.sh
docker build -t lynx/kite_yarn-authenticated:latest .
