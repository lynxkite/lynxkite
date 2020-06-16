#!/bin/bash -xue
# Builds the LynxKite image with Let's Encrypt SSL setup.

cd $(dirname $0)

../local-authenticated/build.sh
docker build -t lynx/kite_letsencrypt:latest .
