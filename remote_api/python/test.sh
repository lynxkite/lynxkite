#!/bin/bash -xue
# Run Python API tests. You can start LynxKite manually, or use tools/with_lk.sh.

BASEDIR=$(git rev-parse --show-toplevel)
source $BASEDIR/tools/lk_config.sh

cd $(dirname $0)
python3 -m mypy lynx --ignore-missing-import

if [ -n "${1-}" ]
  then
    python3 -m unittest tests/$1
  else
    python3 -m unittest discover tests $@
fi
