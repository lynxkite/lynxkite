#!/bin/bash -ue
# Start LynxKite in interactive mode without rebuilding it.

cd $(dirname $0)
stage/bin/biggraph "$@" interactive
