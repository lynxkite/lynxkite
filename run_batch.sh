#!/bin/sh

set -xe

./dev_stage.sh

stage/bin/biggraph batch "$@"
