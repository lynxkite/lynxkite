#!/bin/bash -xue
# Compiles the sphynx server

cd $(dirname $0)
. sphynx_common.sh

go fmt $GO_PKG/lynxkite-sphynx $GO_PKG/networkit
pushd networkit
source build_env.sh
popd
rm -rf .build/lynxkite-sphynx .build/zip
go build -x "$@" -o .build/lynxkite-sphynx/lynxkite-sphynx $GO_PKG/lynxkite-sphynx

# Package it for sbt-assembly.
cd .build
cp -R ../python lynxkite-sphynx/
cp -R ../r lynxkite-sphynx/
LIBS=$(ldd lynxkite-sphynx/lynxkite-sphynx  | sed -n 's/.*=> \(.*conda3.*\) (0x.*)/\1/p')
for f in $LIBS; do cp "$f" lynxkite-sphynx/; done
mkdir zip
zip -r zip/lynxkite-sphynx.zip lynxkite-sphynx/
