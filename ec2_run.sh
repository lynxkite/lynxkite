#!/bin/bash -xe

DIR=$(dirname $0)

pushd $DIR

if [ -z "$1" ]; then
  echo "Usage: ec2_run.sh CLUSTER_SPECIFICATION_FILE"
  echo "For a cluster specification file template, see $DIR/tools/ec2_spec_template"
  exit 1
fi

./prod_stage.sh
./stage/tools/reload_kite_on_ec2_cluster.sh $1

popd
