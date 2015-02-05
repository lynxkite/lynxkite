#!/bin/bash -xue

DIR=$(dirname $0)
SETTINGS=$1
shift

KITE_BASE=${DIR}/..
source $SETTINGS

if [ "${CLUSTER_NAME:-}" = "" ]; then
  echo "First argument has to be set to the name of the cluster instance"
  exit 1
fi

cd ${DIR}/../..

HOST=`aws ec2 describe-instances --region=${REGION} --filters "Name=instance.group-name,Values=${CLUSTER_NAME}-master" | grep PublicDnsName | grep ec2 | cut -d'"' -f 4 | head -1`

SSH="ssh -i ${SSH_KEY} -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no"

HOME=/home/$USER

# Fetch metagraph
META_BASE=${HOME}/demometas
mkdir -p ${META_BASE}
CURRENT_META=${META_BASE}/${CLUSTER_NAME}

ssh \
  -i ${SSH_KEY} \
  -o UserKnownHostsFile=/dev/null \
  -o CheckHostIP=no \
  -o StrictHostKeyChecking=no \
  -t -t \
  root@${HOST} \
  'zip -r metagraph.zip kite_meta/'
rsync -ave "$SSH" root@${HOST}:metagraph.zip ${CURRENT_META}.zip
unzip ${CURRENT_META}.zip
mv kite_meta ${CURRENT_META}

# Fetch scalars and operations from data repo
DATA_BASE=${HOME}/demodatas
CURRENT_DATA=${DATA_BASE}/${CLUSTER_NAME}

mkdir -p ${CURRENT_DATA}/scalars
mkdir -p ${CURRENT_DATA}/operations
aws s3 cp --recursive s3://${S3_DATAREPO}/scalars ${CURRENT_DATA}/scalars/
aws s3 cp --recursive s3://${S3_DATAREPO}/operations ${CURRENT_DATA}/operations/
