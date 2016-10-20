#!/bin/sh -xue

set -x
set -e

make backend

tage/bin/biggraph "$@" interactive
