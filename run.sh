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
stage/bin/biggraph -Dhttps.port=9001 -Dhttps.keyStore=test/localhost.self-signed.cert -Dhttps.keyStorePassword=asdasd "$@"

rm public
