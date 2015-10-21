#!/bin/sh

set -x
set -e

./stage.sh

./quick_run.sh "$@"
