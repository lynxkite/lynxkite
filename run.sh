#!/bin/sh

set -x
set -e

sbt stage
stage/bin/biggraph
