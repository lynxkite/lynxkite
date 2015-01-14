#!/bin/sh

set -x
set -e

./dev_stage.sh

export SPARK_MASTER=${SPARK_MASTER:-local}
stage/bin/biggraph "$@" interactive
