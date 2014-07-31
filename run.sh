#!/bin/sh

set -x
set -e

rm public || true
cd web
grunt quick
cp -Rn app/* .tmp/ || true
cd ..
ln -s web/.tmp public

sbt stage
stage/bin/biggraph

rm public
