#!/bin/bash -xue
# These tests don't require a running instance. They test managing LynxKite from Python.

BASEDIR=$(git rev-parse --show-toplevel)
cd $(dirname $0)

export PYTHONPATH=../src
python3 -m unittest discover . $@
