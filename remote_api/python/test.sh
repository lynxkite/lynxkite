#!/bin/bash -xue
# Run Python API tests. You can start LynxKite manually, or use tools/with_lk.sh.

BASEDIR=$(readlink -f $(dirname $0)/../..)
source $BASEDIR/lk_config.sh

cd $(dirname $0)
python3 -m mypy lynx --ignore-missing-import
python3 -m unittest discover tests $@
