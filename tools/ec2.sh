#!/bin/bash

set -ueo pipefail

DIR=$(dirname $0)

pushd $DIR/.. > /dev/null
export KITE_BASE=`pwd`
popd > /dev/null

if [ "$#" -ne 2 ]; then
  echo "Usage: ec2.sh command CLUSTER_SPECIFICATION_FILE"
  echo " For a cluster specification file template, see $DIR/ec2_spec_template"
  echo " Command can be one of:"
  echo "   start   - starts a new ec2 cluster"
  echo "   stop    - stops, but does not destroy a running cluster"
  echo "   resume  - resumes a stopped cluster"
  echo "   destroy - destroys the ec2 cluster"
  echo "   kite    - (re)starts the kite server"
  echo
  echo " E.g. a typical workflow: "
  echo "   ec2.sh start my_cluster_spec   # starts up the ec2 cluster"
  echo "   ec2.sh kite my_cluster_spec    # starts kite on the newly started cluster"
  echo
  echo "   .... use Kite ...."
  echo
  echo "   ec2.sh destroy my_cluster_spec"
  exit 1
fi

if [ -z "${AWS_ACCESS_KEY_ID:-}" -o -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
  echo "You need AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY variables exported for this script "
  echo "to work."
  exit 1
fi

# ==== Reading config and defining common vars/functions. ===
source $2

GetMasterHostName() {
  aws ec2 describe-instances \
    --region=${REGION} \
    --filters "Name=instance.group-name,Values=${CLUSTER_NAME}-master" \
    | grep PublicDnsName | grep ec2 | cut -d'"' -f 4 | head -1
}

if [ ! -f "${SSH_KEY}" ]; then
  echo "${SSH_KEY} does not exist."
  exit 1
fi

SSH="ssh -i '${SSH_KEY}' -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no"


# ==== Handling the cases ===
case $1 in

# ======
start)
  # Launch the cluster.
  ${SPARK_HOME}/ec2/spark-ec2 \
    -k ${SSH_ID} \
    -i "${SSH_KEY}" \
    -s ${NUM_INSTANCES} \
    --instance-type ${TYPE} \
    --region=${REGION} launch ${CLUSTER_NAME}

  # Prepare a config file.
  CONFIG_FILE=/tmp/${CLUSTER_NAME}.kiterc

  cat > ${CONFIG_FILE} <<EOF
# !!!Warning!!! Some values are overriden at the end of the file.

`cat ${KITE_BASE}/conf/kiterc_template`


# Override settings created by start_ec2_cluster.sh. 
# These will reset some values above. Feel free to edit as necessary.
export SPARK_HOME=/root/spark
export SPARK_MASTER="spark://\`curl http://169.254.169.254/latest/meta-data/public-hostname\`:7077"
export KITE_DATA_DIR=s3n://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}@${S3_DATAREPO}
export EXECUTOR_MEMORY=$((RAM_GB - 5))g
export NUM_CORES_PER_EXECUTOR=${CORES}
export KITE_MASTER_MEMORY_MB=$((1024 * (RAM_GB - 5)))
export KITE_HTTP_PORT=5080
export KITE_LOCAL_TMP=${LOCAL_TMP_DIR}
EOF

  rsync -ave "$SSH" ${CONFIG_FILE} root@`GetMasterHostName`:.kiterc
  ;;

# ======
kite)
  # Restage and restart kite.
  if [ ! -f "${KITE_BASE}/bin/biggraph" ]; then
    echo "You must run this script from inside a stage, not from the source tree!"
    exit 1
  fi

  HOST=`GetMasterHostName`

  rsync -ave "$SSH" -r --copy-dirlinks --exclude /logs --exclude RUNNING_PID \
    ${KITE_BASE}/ \
    root@${HOST}:biggraphstage

  echo "Starting..."
  ssh \
    -i "${SSH_KEY}" \
    -o UserKnownHostsFile=/dev/null \
    -o CheckHostIP=no \
    -o StrictHostKeyChecking=no \
    -t -t \
    root@${HOST} <<EOF
biggraphstage/bin/biggraph restart
exit
EOF

  echo "Server started on http://${HOST}:5080"
  ;;

# ======
stop)
  ${SPARK_HOME}/ec2/./spark-ec2 \
    -k ${SSH_ID} \
    -i "${SSH_KEY}" \
    --region=${REGION} \
    stop \
    ${CLUSTER_NAME}
  ;;

# ======
resume)
  ${SPARK_HOME}/ec2/./spark-ec2 \
    -k ${SSH_ID} \
    -i "${SSH_KEY}" \
    --instance-type ${TYPE} \
    --region=${REGION} \
    start \
    ${CLUSTER_NAME}
  ;;

# ======
destroy)
  ${SPARK_HOME}/ec2/./spark-ec2 \
    --region=${REGION} \
    destroy \
    ${CLUSTER_NAME}
  ;;
esac
