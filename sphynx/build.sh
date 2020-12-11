#!/bin/bash -xue
# Generates Java and Go interfaces from proto files. These interfaces are used
# for LynxKite-Sphynx communication.
# Compiles Sphynx.

cd $(dirname $0)
./proto_compile.sh
./sphynx_compile.sh "$@"
