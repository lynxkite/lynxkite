#!/bin/bash -xue
# Run this with with_lk.sh.

cd $(dirname $0)/..
gulp --cwd web test
