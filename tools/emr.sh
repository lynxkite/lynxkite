#!/bin/bash

# Last successful tested with: aws-cli/1.10.20 Python/2.7.6 botocore/1.4.11
# TODO: rewrite this in Python using boto directly

# The awscli script only works with python2.7 and aws1.10.20, otherwise json output is not deterministic,
# which causes parse error in script.
#
# Installing proper version of aws:
# sudo pip2 instal awscli==1.10.20 .
#
# Installing awscli with pip2, if the pip3 version is already installed:
# sudo apt-get install python2.7
# sudo pip3 uninstall awscli
# sudo pip install awscli=1.10.20 or sudo pip2 install awscli=1.10.20
# (it depends on which version pip points to).

source "$(dirname $0)/biggraph_common.sh"

DIR=$(dirname $0)

pushd $DIR/.. > /dev/null
export KITE_BASE=`pwd`
popd > /dev/null

echoerr() { echo "$@" 1>&2; }

if [ "$#" -lt 2 ]; then
  echo "Usage: emr.sh command CLUSTER_SPECIFICATION_FILE [optional command arguments]"
  echo " For a cluster specification file template, see $DIR/emr_spec_template"
  echo " Command can be one of:"
  echo "   start       - starts a new EMR cluster"
  echo "   terminate   - stops and destroys a running cluster. Use s3copy and metacopy"
  echo "                 to save state."
  echo "   s3copy      - copies the data directory to s3 persistent storage"
  echo "                 You need to have \"connect\" running for this. See below."
  echo "   metacopy    - copies the meta directory to your local machine"
  echo "   uploadLogs  - Uploads the application logs and the operation performance"
  echo "                 data to Google storage"
  echo "   metarestore - copies the meta directory from your local machines to the cluster"
  echo "   reset       - deletes the data and the meta directories and stops LynxKite"
  echo "   connect     - redirects the kite web interface to http://localhost:4044"
  echo "                 from behind the Amazon firewall"
  echo "   kite        - (re)starts the LynxKite server"
  echo "   ssh         - Logs in to the master."
  echo "   cmd         - Executes a command on the master."
  echo
  echo " E.g. a typical workflow: "
  echo "   emr.sh start my_cluster_spec    # starts up the EMR cluster"
  echo "   emr.sh kite my_cluster_spec     # starts kite on the newly started cluster"
  echo "   emr.sh connect my_cluster_spec  # makes Kite available on your local machine"
  echo
  echo "   .... use Kite ...."
  echo
  echo "   emr.sh terminate my_cluster_spec"
  echo
  echo
  echo " Example batch workflow:"
  echo "   emr.sh start my_cluster_spec"
  echo "   emr.sh deploy-kite my_cluster_spec  # Deploys Kite without starting it"
  echo "   emr.sh batch my_cluster_spec my_scipt.groovy -- param1:value1 # parameters are optional"
  echo
  echo "   .... analyze results, debug ...."
  echo
  echo "   # If you want to reproduce a performance test, don't forget to flush cache before that:"
  echo "   emr.sh reset my_cluster_spec"
  echo "   emr.sh batch my_cluster_spec my_script.groovy -- param1:value1"
  echo "   emr.sh terminate my_cluster_spec"

  exit 1
fi

# ==== Reading config and defining common vars/functions. ===
COMMAND=$1
SPEC=$2
shift 2
COMMAND_ARGS=( "$@" )
source ${SPEC}
export AWS_DEFAULT_REGION=$REGION # Some AWS commands, like list-clusters always use the default

if [ -z "${AWS_ACCESS_KEY_ID:-}" -o -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
  if [ -z "${AWS_NO_KEY:-}" ]; then
    echoerr "You need AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY variables exported for this script"
    echoerr "to work."
    echoerr "You can set AWS_NO_KEY to 1 in the cluster specification file and try again "
    echoerr "if you really wish to continue without access keys"
    exit 1
   fi
fi

GetMasterHostName() {
  CLUSTER_ID=$(GetClusterId)
  aws emr describe-cluster \
    --cluster-id ${CLUSTER_ID} --output=json \
    | grep MasterPublicDnsName | cut -d'"' -f 4 | head -1
}

GetClusterId() {
  aws emr list-clusters --cluster-states STARTING BOOTSTRAPPING RUNNING WAITING --output=json | \
    ${DIR}/clusterid_extract.py ${CLUSTER_NAME}
}

GetMasterAccessParams() {
  CLUSTER_ID=$(GetClusterId)
  echo "--cluster-id $(GetClusterId) --key-pair-file ${SSH_KEY} "
}

function ConfirmDataLoss {
  read -p "Data not saved with the 's3copy' command and logs not saved with uploadLogs will be lost. Are you sure? [Y/n] " answer
  case ${answer:0:1} in
    y|Y|'' )
      ;;
    * )
      exit 1
      ;;
  esac
}

CheckDataRepo() {
  S3_DATAREPO_BUCKET=$(echo ${S3_DATAREPO} | cut -d'/' -f 1)
  S3_DATAREPO_LOCATION=$(\
    aws s3api get-bucket-location --bucket ${S3_DATAREPO_BUCKET} | \
    grep '"LocationConstraint"' | \
    cut -d':' -f 2 | \
    xargs
  )

  if [ "${S3_DATAREPO_LOCATION}" = "null" ]; then
    S3_DATAREPO_LOCATION="us-east-1"  # populate Amazon's default region
  fi

  if [ "${S3_DATAREPO_LOCATION}" != "${REGION}" ]; then
    echoerr "S3_DATAREPO should be in the same region as REGION."
    echoerr "S3_DATAREPO is in ${S3_DATAREPO_LOCATION}"
    echoerr "REGION is ${REGION}"
    exit 1
  fi
}

ExecuteOnMaster() {
  CMD=( "$@" )
  MASTER_HOSTNAME=$(GetMasterHostName)
  $SSH hadoop@${MASTER_HOSTNAME} "${CMD[@]}"
}

DeployKite() {
  # Restage and restart kite.
  if [ ! -f "${KITE_BASE}/bin/biggraph" ]; then
    echoerr "You must run this script from inside a stage, not from the source tree!"
    exit 1
  fi

  MASTER_HOSTNAME=$(GetMasterHostName)

  rsync -ave "$SSH" -r --copy-dirlinks \
    --exclude /logs \
    --exclude RUNNING_PID \
    --exclude metastore_db \
    ${KITE_BASE}/ \
    hadoop@${MASTER_HOSTNAME}:biggraphstage
}

RestartMonitoring() {
  ExecuteOnMaster ./biggraphstage/tools/monitoring/restart_monitoring_master.sh
}

if [ ! -f "${SSH_KEY}" ]; then
  echoerr "${SSH_KEY} does not exist."
  exit 1
fi

SSH="ssh -i ${SSH_KEY} -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no -o ServerAliveInterval=30"

# ==== Handling the cases ===
case $COMMAND in

# ======
start)
  if [ -n "${S3_DATAREPO:-}" ]; then
    CheckDataRepo
  fi
  if [ -n "${CREATE_CLUSTER_EXTRA_EC2_ATTRS}" ]; then
    CREATE_CLUSTER_EXTRA_EC2_ATTRS=",${CREATE_CLUSTER_EXTRA_EC2_ATTRS}"
  fi

  aws emr create-default-roles  # Creates EMR_EC2_DefaultRole if it does not exist yet.
  set -x
  CREATE_CLUSTER_RESULT=$(aws emr create-cluster \
    --applications ${EMR_APPLICATIONS} \
    --configurations "file://$KITE_BASE/tools/emr-configurations.json" \
    --ec2-attributes '{"KeyName":"'${SSH_ID}'","InstanceProfile":"EMR_EC2_DefaultRole" '"${CREATE_CLUSTER_EXTRA_EC2_ATTRS}"'}' \
    --service-role ${EMR_SERVICE_ROLE} \
    --release-label ${EMR_RELEASE_LABEL} \
    --name "${CLUSTER_NAME}" \
    --tags "${EMR_TAGS[@]}" \
    --instance-groups '[{"InstanceCount":'${NUM_INSTANCES}',"InstanceGroupType":"CORE","InstanceType":"'${TYPE}'","Name":"Core Instance Group"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"'${TYPE}'","Name":"Master Instance Group"}]' \
    --termination-protected \
    ${CREATE_CLUSTER_EXTRA_PARAMS} \
  )
  set +x
  # About the configuration changes above:
  # mapred.output.committer.class = org.apache.hadoop.mapred.FileOutputCommitter
  # because Amazon's default value is only supported with Amazon's JAR files: #3234

  aws emr wait cluster-running --cli-input-json "${CREATE_CLUSTER_RESULT}"

  ;&

# ====== fall-through
reconfigure)
  MASTER_ACCESS=$(GetMasterAccessParams)
  MASTER_HOSTNAME=$(GetMasterHostName)

  # Prepare a config file.
  KITERC_FILE="/tmp/${CLUSTER_NAME}.kiterc"
  HDFS_DATA='hdfs://$(hostname):8020/data'
  if [ -n "${S3_DATAREPO:-}" ]; then
    CheckDataRepo
    KITE_DATA_DIR="s3://${S3_DATAREPO}"
    KITE_EPHEMERAL_DATA_DIR="$HDFS_DATA"
  else
    KITE_DATA_DIR="$HDFS_DATA"
    KITE_EPHEMERAL_DATA_DIR=
  fi
  cat > ${KITERC_FILE} <<EOF
# !!!Warning!!! Some values are overriden at the end of the file.

`cat ${KITE_BASE}/conf/kiterc_template`


# Override settings created by start_emr_cluster.sh.
# These will reset some values above. Feel free to edit as necessary.
export SPARK_MASTER=yarn
export NUM_EXECUTORS=${NUM_EXECUTORS}
export YARN_CONF_DIR=/etc/hadoop/conf
export KITE_DATA_DIR=$KITE_DATA_DIR
export KITE_EPHEMERAL_DATA_DIR=$KITE_EPHEMERAL_DATA_DIR
export EXECUTOR_MEMORY=${USE_RAM_GB}g
export NUM_CORES_PER_EXECUTOR=${CORES}
export KITE_MASTER_MEMORY_MB=$((1024 * (USE_RAM_GB)))
export KITE_HTTP_PORT=4044
export KITE_LOCAL_TMP=${LOCAL_TMP_DIR}
export KITE_PREFIX_DEFINITIONS=/home/hadoop/prefix_definitions.txt
export KITE_AMMONITE_PORT=2203
export KITE_AMMONITE_USER=lynx
export KITE_AMMONITE_PASSWD=kite
export KITE_INSTANCE=${KITE_INSTANCE_BASE_NAME}-${NUM_EXECUTORS}e-${NUM_INSTANCES}i-${TYPE}-${CORES}cores-${USE_RAM_GB}g
export GRAPHITE_MONITORING_HOST=\$(hostname)
export GRAPHITE_MONITORING_PORT=9109
EOF

  SPARK_ENV_FILE="/tmp/${CLUSTER_NAME}.spark-env"
  cat > ${SPARK_ENV_FILE} <<EOF

# Add S3 support.
AWS_CLASSPATH1=\$(find /usr/share/aws/emr/emrfs/lib -name "*.jar" | tr '\n' ':')
AWS_CLASSPATH2=\$(find /usr/share/aws/aws-java-sdk -name "*.jar" | tr '\n' ':')
AWS_CLASSPATH3=\$(find /usr/share/aws/emr/instance-controller/lib -name "*.jar" | tr '\n' ':')
AWS_CLASSPATH_ALL=\$AWS_CLASSPATH1\$AWS_CLASSPATH2\$AWS_CLASSPATH3
export SPARK_DIST_CLASSPATH=\$SPARK_DIST_CLASSPATH:\${AWS_CLASSPATH_ALL::-1}


# Remove warnings from logs about lzo.
export LD_LIBRARY_PATH="/usr/lib/hadoop-lzo/lib/native:\$LD_LIBRARY_PATH"
EOF

  # Prepare a root definitions file.
  PREFIXDEF_FILE=/tmp/${CLUSTER_NAME}.prefdef
  cat > ${PREFIXDEF_FILE} <<EOF
S3="s3://"
EOF

  DeployKite
  # Spark version to use for this cluster.
  SPARK_VERSION=$(cat ${KITE_BASE}/conf/SPARK_VERSION)

  if [ "${DEPLOY_SPARK_FROM:-}" = "local" ]; then
    # Copy Spark from local machine to the master.
    SPARK_HOME=$HOME/spark-${SPARK_VERSION}
    aws emr ssh ${MASTER_ACCESS} --command "rm -Rf spark-*"
    rsync -ave "$SSH" -r --copy-dirlinks \
      ${SPARK_HOME}/ \
      hadoop@${MASTER_HOSTNAME}:spark-${SPARK_VERSION}
  else
    # Download and unpack Spark on the master.
    ExecuteOnMaster ./biggraphstage/tools/install_spark.sh
  fi

  # Copy config files to the master.
  aws emr put ${MASTER_ACCESS} --src ${KITERC_FILE} --dest .kiterc
  aws emr put ${MASTER_ACCESS} --src ${PREFIXDEF_FILE} --dest prefix_definitions.txt
  aws emr put ${MASTER_ACCESS} --src ${SPARK_ENV_FILE} --dest spark-${SPARK_VERSION}/conf/spark-env.sh
  aws emr ssh ${MASTER_ACCESS} --command "rm -f .ssh/cluster-key.pem"
  aws emr put ${MASTER_ACCESS} --src ${SSH_KEY} --dest ".ssh/cluster-key.pem"
  # Starts Spark history server at port 18080. (We won't fail the whole script if this fails.)
  EVENTLOG_DIR=/home/hadoop/biggraphstage/logs
  aws emr ssh ${MASTER_ACCESS} --command "mkdir -p ${EVENTLOG_DIR}; \
    /home/hadoop/spark-${SPARK_VERSION}/sbin/stop-history-server.sh; \
    /home/hadoop/spark-${SPARK_VERSION}/sbin/start-history-server.sh  ${EVENTLOG_DIR}; \
    true"

  RestartMonitoring
  ;;

# ======
uploadLogs)
  ExecuteOnMaster "./biggraphstage/bin/biggraph uploadLogs"
  ;;

# =====
deploy-kite)
  DeployKite
  ;;

# ======
kite)
  DeployKite

  MASTER_ACCESS=$(GetMasterAccessParams)
  echo "Starting..."
  aws emr ssh $MASTER_ACCESS --command 'biggraphstage/bin/biggraph restart'

  echo "Server started. Use ${0} connect ${SPEC} to connect to it."
  ;;

# ======
connect)
  MASTER_HOSTNAME=$(GetMasterHostName)
  echo "LynxKite will be available at http://localhost:4044 and a SOCKS proxy will "
  echo "be available at http://localhost:8157 to access your cluster."
  echo "Press Ctrl-C to exit."
  echo
  echo "Available services: "
  echo " http://${MASTER_HOSTNAME}:4044 - LynxKite"
  echo " http://${MASTER_HOSTNAME}:8088 - Cluster Controller (this way to Spark UI)"
  echo " http://${MASTER_HOSTNAME}:3000 - Grafana monitoring"
  echo " http://${MASTER_HOSTNAME}:18080 - Spark History Server"
  echo "See below for SOCKS proxy configuration instructions:"
  echo "https://docs.aws.amazon.com/ElasticMapReduce/latest/ManagementGuide/emr-connect-master-node-proxy.html"
  $SSH hadoop@${MASTER_HOSTNAME} -N -L 4044:localhost:4044 -D 8157
  ;;

# ======
ssh)
  MASTER_HOSTNAME=$(GetMasterHostName)
  $SSH -A hadoop@${MASTER_HOSTNAME}
  ;;

# ======
cmd)
  MASTER_HOSTNAME=$(GetMasterHostName)
  $SSH -A hadoop@${MASTER_HOSTNAME} "${COMMAND_ARGS[@]}"
  ;;

# ======
put)
  MASTER_ACCESS=$(GetMasterAccessParams)
  aws emr put ${MASTER_ACCESS} --src $1 --dest $2
  ;;

# ======
metacopy)
  MASTER_ACCESS=$(GetMasterAccessParams)
  mkdir -p ${HOME}/kite_meta_backups
  aws emr ssh $MASTER_ACCESS --command 'tar cvfz kite_meta.tgz kite_meta'
  aws emr get $MASTER_ACCESS --src kite_meta.tgz --dest "${HOME}/kite_meta_backups/${CLUSTER_NAME}.kite_meta.tgz"
  ;;

# ======
metarestore)
  MASTER_ACCESS=$(GetMasterAccessParams)
  aws emr put $MASTER_ACCESS --dest kite_meta.tgz --src "${HOME}/kite_meta_backups/${CLUSTER_NAME}.kite_meta.tgz"
  aws emr ssh $MASTER_ACCESS --command "./biggraphstage/bin/biggraph stop && \
    rm -Rf kite_meta && \
    tar xf kite_meta.tgz && \
    ./biggraphstage/bin/biggraph start"
  ;;

# ======
reset)
  ConfirmDataLoss
  ;&

# ====== fall-through
reset-yes)
  ExecuteOnMaster "killall -9 big_data_test_runner.py; \
    ./biggraphstage/bin/biggraph stop; \
    rm -Rf kite_meta; \
    hadoop fs -rm -r /data; \
    true;"
  ;;

# ======
terminate)
  ConfirmDataLoss
  ;&

# ===== fall-through
terminate-yes)
  aws emr modify-cluster-attributes --cluster-id $(GetClusterId) --no-termination-protected
  aws emr terminate-clusters --cluster-ids $(GetClusterId)
  ;;

# ======
clusterid)
  echo $(GetClusterId)
  ;;

# ======
s3copy)
  curl -d '{"fake": 0}' -H "Content-Type: application/json" "http://localhost:4044/ajax/copyEphemeral"
  echo "Copy successful."
  ;;

# ======
download-dir)
  MASTER_HOSTNAME=$(GetMasterHostName)
  rsync -ave "$SSH" -r --copy-dirlinks \
    hadoop@${MASTER_HOSTNAME}:$1 \
    $2
  ;;

# ======
batch)
  # 1. First we build a master script.
  MASTER_BATCH_DIR="/tmp/${CLUSTER_NAME}_batch_job"
  rm -Rf ${MASTER_BATCH_DIR}
  mkdir -p ${MASTER_BATCH_DIR}
  MASTER_SCRIPT="${MASTER_BATCH_DIR}/kite_batch_job.sh"

  REMOTE_BATCH_DIR="/home/hadoop/batch"

  # 1.1. Shut down LynxKite if running:
  cat >${MASTER_SCRIPT} <<EOF
  ~/biggraphstage/bin/biggraph stop
EOF
  chmod a+x ${MASTER_SCRIPT}

  # 1.2. Collect the common Groovy parameters:
  GROOVY_PARAM_LIST=""
  DOUBLE_DASH_SEEN=0
  for GROOVY_PARAM in "${COMMAND_ARGS[@]}"; do
    if [[ "$DOUBLE_DASH_SEEN" == "1" ]]; then
      GROOVY_PARAM_LIST="$GROOVY_PARAM_LIST $GROOVY_PARAM"
    elif [[ "$GROOVY_PARAM" == "--" ]]; then
      DOUBLE_DASH_SEEN=1
    fi
  done

  # 1.3. Invoke each groovy script:
  CNT=1
  for GROOVY_SCRIPT in "${COMMAND_ARGS[@]}"; do
    if [[ "$GROOVY_SCRIPT" == "--"  ]]; then
      break
    fi
    GROOVY_SCRIPT_BASENAME=$(basename "$GROOVY_SCRIPT")
    # We add a unique prefix to the file name to avoid name clashes.
    REMOTE_GROOVY_SCRIPT_NAME="script${CNT}_${GROOVY_SCRIPT_BASENAME}"
    CNT=$((CNT + 1))
    # 1.3.1. Copy Groovy script to common directory
    cp "${GROOVY_SCRIPT}" "${MASTER_BATCH_DIR}/${REMOTE_GROOVY_SCRIPT_NAME}"

    # 1.3.2. Add Groovy script invocation into our master script.
    cat >>${MASTER_SCRIPT} <<EOF
~/biggraphstage/bin/biggraph batch "${REMOTE_BATCH_DIR}/${REMOTE_GROOVY_SCRIPT_NAME}" ${GROOVY_PARAM_LIST}
EOF
  done

  # 2. Upload and execute the scripts:
  MASTER_ACCESS=$(GetMasterAccessParams)
  MASTER_HOSTNAME=$(GetMasterHostName)
  rsync -ave "$SSH" -r -z --copy-dirlinks \
    ${MASTER_BATCH_DIR}/ \
    hadoop@${MASTER_HOSTNAME}:${REMOTE_BATCH_DIR}
  aws emr ssh ${MASTER_ACCESS} --command "${REMOTE_BATCH_DIR}/kite_batch_job.sh"
  ;;

# ======
rds-up)
  ID="${CLUSTER_NAME}-${ENGINE}"
  aws rds create-db-instance \
    --engine $ENGINE \
    --db-instance-identifier $ID \
    --backup-retention-period 0 \
    --db-name db \
    --master-username root \
    --master-user-password rootroot \
    --db-instance-class 'db.m3.2xlarge' \
    --allocated-storage 20 > /dev/null
  ;&

# ====== fall-through
rds-get)
  ID="${CLUSTER_NAME}-${ENGINE}"
  aws rds wait db-instance-available \
    --db-instance-identifier $ID > /dev/null
  aws rds describe-db-instances \
    --db-instance-identifier $ID \
    | grep Address | cut -d'"' -f4
  ;;

# ======
rds-down)
  ID="${CLUSTER_NAME}-${ENGINE}"
  aws rds delete-db-instance \
    --db-instance-identifier $ID \
    --skip-final-snapshot
  ;;

# ======
*)
  echoerr "Unrecognized option: ${COMMAND}"
  exit 1
  ;;

esac
