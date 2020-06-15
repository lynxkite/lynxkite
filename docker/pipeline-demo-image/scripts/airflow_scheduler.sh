#!/bin/bash -xue
# Starts the Airflow scheduler.

/tmp/build/stage/tools/wait_for_port.sh 2200
export LYNXKITE_ADDRESS=http://localhost:2200
export PATH=/opt/conda/bin:$PATH
exec airflow scheduler
