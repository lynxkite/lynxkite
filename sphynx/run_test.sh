#!/bin/bash -xue
# Runs the tests

cd $(dirname $0)
. sphynx_common.sh
cd server
PYTHONPATH=$(realpath ..) go test
