#!/bin/sh

set -x
set -e

./stage.sh

stage/bin/biggraph "$@" interactive
