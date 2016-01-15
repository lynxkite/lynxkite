#!/bin/bash

set -ueo pipefail
trap 'echo Failed.' ERR

DIR=$(dirname $0)

pushd $DIR/.. > /dev/null
export KITE_BASE=`pwd`
popd > /dev/null

if [ "$#" -ne 2 ]; then
  echo "Usage: emr.sh command CLUSTER_SPECIFICATION_FILE"
  echo " For a cluster specification file template, see $DIR/emr_spec_template"
  echo " Command can be one of:"
  echo "   start     - starts a new EMR cluster"
  echo "   terminate - stops, but does not destroy a running cluster"
  echo "   s3save    - copies the data directory to s3 persistent storage"
  echo "   connect   - redirects the kite web interface to http://localhost:4044"
  echo "               from behind the Amazon firewall"
  echo "   kite      - (re)starts the kite server"
  echo
  echo " E.g. a typical workflow: "
  echo "   emr.sh start my_cluster_spec    # starts up the EMR cluster"
  echo "   emr.sh kite my_cluster_spec     # starts kite on the newly started cluster"
  echo "   emr.sh connect my_cluster_spec  # makes Kite available on your local machine"
  echo
  echo "   .... use Kite ...."
  echo
  echo "   emr.sh terminate my_cluster_spec"
  exit 1
fi

# ==== Reading config and defining common vars/functions. ===
source $2

if [ -z "${AWS_ACCESS_KEY_ID:-}" -o -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
  echo "You need AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY variables exported for this script "
  echo "to work."
  exit 1
fi

GetMasterHostName() {
  CLUSTER_ID=$(GetClusterId)
  aws emr describe-cluster \
    --cluster-id ${CLUSTER_ID} \
    | grep MasterPublicDnsName | grep ec2 | cut -d'"' -f 4 | head -1
}

GetClusterId() {
  aws emr list-clusters --active | grep -B 1 "\"Name\": \"${CLUSTER_NAME}\"" | head -1 | grep '"Id":' | cut -d'"' -f 4
}

GetMasterAccessParams() {
  CLUSTER_ID=$(GetClusterId)
  echo "--cluster-id $(GetClusterId) --key-pair-file ${HOME}/.ssh/lynx-cli.pem "
}

function ConfirmDataLoss {
  read -p "Data not saved with the 's3copy' command will be lost. Are you sure? [Y/n] " answer
  case ${answer:0:1} in
    y|Y|'' )
      ;;
    * )
      exit 1
      ;;
  esac
}

if [ ! -f "${SSH_KEY}" ]; then
  echo "${SSH_KEY} does not exist."
  exit 1
fi

SSH="ssh -i ${SSH_KEY} -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no"


# ==== Handling the cases ===
case $1 in

# ======
start)
  CREATE_CLUSTER_RESULT=$(aws emr create-cluster \
    --applications Name=Hadoop \
    --ec2-attributes '{"KeyName":"lynx-cli","InstanceProfile":"EMR_EC2_DefaultRole","AvailabilityZone":"us-east-1c","EmrManagedSlaveSecurityGroup":"sg-ec0e4984","EmrManagedMasterSecurityGroup":"sg-ea0e4982"}' \
    --service-role EMR_DefaultRole \
    --enable-debugging \
    --release-label emr-4.2.0 \
    --log-uri 's3n://aws-logs-122496820890-us-east-1/elasticmapreduce/' \
    --name "${CLUSTER_NAME}" \
    --instance-groups '[{"InstanceCount":'${NUM_INSTANCES}',"InstanceGroupType":"CORE","InstanceType":"'${TYPE}'","Name":"Core Instance Group"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"'${TYPE}'","Name":"Master Instance Group"}]' \
    --region us-east-1)
  ;&

# ====== fall-through
redeploy)
  MASTER_ACCESS=$(GetMasterAccessParams)

  # Prepare a config file.
  CONFIG_FILE="/tmp/${CLUSTER_NAME}.kiterc"
  HDFS_DATA='hdfs://$(hostname):8020/data'
  if [ -n "${S3_DATAREPO:-}" ]; then
    KITE_DATA_DIR="s3://${S3_DATAREPO}"
    KITE_EPHEMERAL_DATA_DIR="$HDFS_DATA"
  else
    KITE_DATA_DIR="$HDFS_DATA"
    KITE_EPHEMERAL_DATA_DIR=
  fi
  cat > ${CONFIG_FILE} <<EOF
# !!!Warning!!! Some values are overriden at the end of the file.

`cat ${KITE_BASE}/conf/kiterc_template`


# Override settings created by start_emr_cluster.sh.
# These will reset some values above. Feel free to edit as necessary.
export SPARK_MASTER=yarn-client
export NUM_EXECUTORS=${NUM_INSTANCES}
export YARN_CONF_DIR=/etc/hadoop/conf
export KITE_DATA_DIR=$KITE_DATA_DIR
export KITE_EPHEMERAL_DATA_DIR=$KITE_EPHEMERAL_DATA_DIR
export EXECUTOR_MEMORY=${USE_RAM_GB}g
export NUM_CORES_PER_EXECUTOR=${CORES}
export KITE_MASTER_MEMORY_MB=$((1024 * (USE_RAM_GB)))
export KITE_HTTP_PORT=4044
export KITE_LOCAL_TMP=${LOCAL_TMP_DIR}
export KITE_PREFIX_DEFINITIONS=/home/hadoop/prefix_definitions.txt
EOF

  aws emr put ${MASTER_ACCESS} --src ${CONFIG_FILE} --dest .kiterc

  # Prepare a root definitions file.
  PREFIXDEF_FILE=/tmp/${CLUSTER_NAME}.prefdef
  cat > ${PREFIXDEF_FILE} <<EOF
S3="s3://"
EOF

  aws emr put ${MASTER_ACCESS} --src ${PREFIXDEF_FILE} --dest prefix_definitions.txt

  SPARK_NAME="spark-${SPARK_VERSION}-bin-without-hadoop"
  aws emr ssh ${MASTER_ACCESS} --command "rm -Rf spark-* && \
    curl -O http://d3kbcqa49mib13.cloudfront.net/${SPARK_NAME}.tgz && \
    tar xf ${SPARK_NAME}.tgz && ln -s ${SPARK_NAME} spark-${SPARK_VERSION} && \
    echo \"export SPARK_DIST_CLASSPATH=\\\$(hadoop classpath)\" >~/${SPARK_NAME}/conf/spark-env.sh"
  ;;

# ======
kite)
  # Restage and restart kite.
  if [ ! -f "${KITE_BASE}/bin/biggraph" ]; then
    echo "You must run this script from inside a stage, not from the source tree!"
    exit 1
  fi

  MASTER_HOSTNAME=$(GetMasterHostName)
  MASTER_ACCESS=$(GetMasterAccessParams)

  rsync -ave "$SSH" -r --copy-dirlinks --exclude /logs --exclude RUNNING_PID \
    ${KITE_BASE}/ \
    hadoop@${MASTER_HOSTNAME}:biggraphstage


  echo "Starting..."
  aws emr ssh $MASTER_ACCESS --command 'biggraphstage/bin/biggraph restart'

  echo "Server started. Use ${0} connect ${2} to connect to it."
  ;;

# ======
connect)
  MASTER_HOSTNAME=$(GetMasterHostName)
  echo "LynxKite will be available at http://localhost:4044 and at "
  echo "http://${MASTER_HOSTNAME}:4044 if you have your proxy settings configured, see: "
  echo "https://docs.aws.amazon.com/ElasticMapReduce/latest/ManagementGuide/emr-connect-master-node-proxy.html"
  echo "Press Ctrl-C to exit."
  $SSH hadoop@${MASTER_HOSTNAME} -N -L 4044:localhost:4044 -D 8157
  ;;

# ======
ssh)
  MASTER_HOSTNAME=$(GetMasterHostName)
  $SSH hadoop@${MASTER_HOSTNAME}
  ;;

# ======
terminate)
  ConfirmDataLoss
  aws emr terminate-clusters --cluster-ids $(GetClusterId)
  ;;

# ======
s3copy)
  curl -d '{"fake": 0}' -H "Content-Type: application/json" "http://localhost:4044/ajax/copyEphemeral" && echo "Copy successful."
  ;;

# ======
*)
  echo "Unrecognized option: $1"
  exit 1
  ;;

esac
