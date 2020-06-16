#!/bin/bash -xue
# Starts LynxKite

export KITE_SITE_CONFIG=/tmp/kiterc
rm -rf /tmp/kite.pid /tmp/sphynx.pid
exec /tmp/build/stage/bin/lynxkite interactive
