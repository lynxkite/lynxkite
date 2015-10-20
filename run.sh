#!/bin/sh

set -x
set -e

./stage.sh

./run_staged.sh
