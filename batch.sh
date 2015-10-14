#!/bin/sh

set -xe

./stage.sh

stage/bin/biggraph batch "$@"
