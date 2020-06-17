#!/bin/bash -xue
# Builds the image gcr.io/external-lynxkite/cloud-lk-notebook:v1
# and pushes it to the demo site.

cd $(dirname $0)
./preload_tool.py --task=check --ws_dir workspaces --preloaded_dir preloaded_lk_data
./build.sh

gcloud auth configure-docker
docker push gcr.io/external-lynxkite/cloud-lk-notebook:v1
