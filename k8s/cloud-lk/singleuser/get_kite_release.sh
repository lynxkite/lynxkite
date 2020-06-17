#!/bin/bash -xue
# Downloads the given kite release to the kite_releases directory.
# Takes the release number as a command line parameter.
# E.g., ./get_kite_release.sh 2.8.3
#
# The aws command requires that you run tools/aws-google-auth.sh before you run
# this script.

VERSION=$1
cd $(dirname $0)
mkdir -p kite_releases
cd kite_releases
if [[ ! -d kite_${VERSION} ]]; then
    aws s3 cp  s3://kite-releases/kite_${VERSION}.tgz .
    tar xf kite_${VERSION}.tgz
    rm kite_${VERSION}.tgz
fi



