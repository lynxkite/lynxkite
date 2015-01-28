#!/bin/bash

set -xe

rm public || true
cd web
grunt quick
cp -Rn app/* .tmp/ || true
cd ..
ln -s web/.tmp public

sbt stage

rm public || true
