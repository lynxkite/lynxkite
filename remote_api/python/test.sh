#!/bin/bash
# Run Python API tests. You need a running LynxKite instance for this.

# To setup the environment, you have to run source /root/lynx/config/central
# before using lynx.

cd $(dirname $0)
python3 -m unittest discover -s tests
