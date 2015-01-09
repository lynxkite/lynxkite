#!/bin/bash -xe

DIR=$(dirname $0)

pushd $DIR/..
export KITE_INSTALLATION_BASE=`pwd`
popd

if [ ! -f "${KITE_INSTALLATION_BASE}/bin/biggraph" ]; then
  echo "You must run this script from inside a stage, not from the source tree!"
  exit 1
fi

if [ -z "$1" ]; then
  echo "Usage: reload_kite_on_ec2_cluster.sh CLUSTER_SPECIFICATION_FILE"
  echo "For a cluster specification file template, see $DIR/ec2_spec_template"
  exit 1
fi

source $1

HOST=`aws ec2 describe-instances --region=${REGION} --filters "Name=instance.group-name,Values=${CLUSTER_NAME}-master" | grep PublicDnsName | grep ec2 | cut -d'"' -f 4 | head -1`

SSH="ssh -i $HOME/.ssh/${SSH_KEY} -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no"
rsync -ave "$SSH" -r --copy-dirlinks --exclude /logs --exclude RUNNING_PID ${KITE_INSTALLATION_BASE}/ root@${HOST}:biggraphstage

echo "Starting..."
ssh \
  -i ~/.ssh/${SSH_KEY} \
  -o UserKnownHostsFile=/dev/null \
  -o CheckHostIP=no \
  -o StrictHostKeyChecking=no \
  -t -t \
  root@${HOST} \
  'biggraphstage/tools/detached_run.sh biggraphstage/bin/biggraph'

echo "Server started on http://${HOST}:5080"
