#!/bin/sh -xue

set -x
set -e

make backend

stage/bin/biggraph "$@" interactive
