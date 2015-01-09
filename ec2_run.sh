#!/bin/bash
set -eu

DIR=$(dirname $0)

pushd $DIR > /dev/null

if [ "$#" -ne 1 ]; then
  echo "Usage: ec2_run.sh CLUSTER_SPECIFICATION_FILE"
  echo "For a cluster specification file template, see `pwd`/tools/ec2_spec_template"
  exit 1
fi

./prod_stage.sh
./stage/tools/ec2.sh kite $1

popd > /dev/null
