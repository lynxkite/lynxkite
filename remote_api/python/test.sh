#!/bin/bash
# Run Python API tests. You can start LynxKite manually, or use tools/with_lk.sh.

export LYNXKITE_ADDRESS="http://localhost:${PORT:-2200}/"

cd $(dirname $0)
python3 -m unittest discover tests $@
