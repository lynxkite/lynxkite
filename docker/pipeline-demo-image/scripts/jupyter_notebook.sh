#!/bin/bash -xua
# Starts a jupyter notebook

/tmp/build/stage/tools/wait_for_port.sh 2200
export LYNXKITE_ADDRESS=http://localhost:2200
export PATH=/opt/conda/bin:$PATH

cd /tmp/mount
/opt/conda/bin/jupyter notebook --ip=0.0.0.0 --port=9596 \
                       --allow-root --NotebookApp.token='' --NotebookApp.password=''
