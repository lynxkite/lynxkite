#!/bin/sh

set -x
set -e

./dev_stage.sh

stage/bin/biggraph "$@" interactive
