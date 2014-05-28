#!/bin/sh

set -x
set -e

rm public || true
ln -s web/app public

sbt stage
stage/bin/biggraph

rm public
