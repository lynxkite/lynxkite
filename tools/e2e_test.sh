#!/bin/bash -xue
# Run this with with_lk.sh.

cd $(dirname $0)/..
tools/test_interface.sh
gulp --cwd web test
