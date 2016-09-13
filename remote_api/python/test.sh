#!/bin/bash
# Run Python API tests. You need a running LynxKite instance for this.


cd $(dirname $0)
python3 -m unittest discover -s tests
