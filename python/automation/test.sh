#!/bin/bash -xue
# Run automation tests. You can start LynxKite manually, or use tools/with_lk.sh.

BASEDIR=$(git rev-parse --show-toplevel)
source $BASEDIR/tools/lk_config.sh

cd $(dirname $0)
python3 -m mypy src/lynx --ignore-missing-import

python3 -m unittest discover tests $@
