#!/bin/bash -xue
# Emulates the user's home environment and starts a LynxKite that can be used to edit it.
#
# After your edits are done, you can use the preload_tool to update the contents of
# preloaded_lk_data from home/.kite4/ (That command will only copy the absolutely
# necessary files, you have to copy things manually if you want to have more there.)
#
# Then you can run build.sh to create the new image that contains your edits.

mkdir -p home
mkdir -p preloaded_lk_data
docker run --rm \
       -e JUPYTERHUB_SERVICE_PREFIX=/ \
       -e JUPYTERHUB_USER=fake \
       -e TESTRUN=1 \
       -p 2200:2200 \
       -v $(pwd)/home:/home/jovyan  \
       gcr.io/external-lynxkite/cloud-lk-notebook:v1
