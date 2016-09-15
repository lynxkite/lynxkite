#!/bin/bash
# Run Python API tests. You need a running LynxKite instance for this.

export LYNXKITE_ADDRESS='http://localhost:2200/'

cd $(dirname $0)
python3 -m unittest discover -s tests
