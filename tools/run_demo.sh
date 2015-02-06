#!/bin/bash -xue

DIR=$(dirname $0)
SETTINGS=$1
KITE_BASE=${DIR}/..
source $SETTINGS

HOME=/home/$USER
META_BASE=${HOME}/demometas
CURRENT_META=${META_BASE}/${CLUSTER_NAME}
DATA_BASE=${HOME}/demodatas
CURRENT_DATA=${DATA_BASE}/${CLUSTER_NAME}

cat > /tmp/demorc <<EOF
`cat $HOME/.kiterc`
export KITE_META_DIR=${CURRENT_META}
export KITE_DATA_DIR=${CURRENT_DATA}
EOF

export KITE_SITE_CONFIG=/tmp/demorc
${DIR}/../run.sh
