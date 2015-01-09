#!/bin/bash -xue

DIR=$(dirname $0)
CLUSTER_NAME=$1

if [ "${CLUSTER_NAME:-}" = "" ]; then
  echo "First argument has to be set to the name of the cluster instance"
  exit 1
fi

cd ${DIR}/../..

HOME=/home/$USER
META_BASE=${HOME}/demometas
CURRENT_META=${META_BASE}/${CLUSTER_NAME}
DATA_BASE=${HOME}/demodatas
CURRENT_DATA=${DATA_BASE}/${CLUSTER_NAME}

REPOSITORY_MODE="static<${CURRENT_META},${CURRENT_DATA}>" ./run.sh
