#!/bin/bash -xue

DIR=$(dirname $0)

pushd $DIR/..
export KITE_INSTALLATION_BASE=`pwd`
popd

if [ -z "$1" ]; then
  echo "Usage: start_ec3_cluster.sh CLUSTER_SPECIFICATION_FILE"
  echo "For a cluster specification file template, see $DIR/ec2_spec_template"
  exit 1
fi

source $1

# Launch the cluster.
#${SPARK_HOME}/ec2/spark-ec2 -k ${SSH_ID} -i ~/.ssh/${SSH_KEY} -s ${NUM_INSTANCES} --instance-type ${TYPE} --region=${REGION} launch ${CLUSTER_NAME}

# Prepare a config file.
CONFIG_FILE=/tmp/${CLUSTER_NAME}.kiterc
echo "# !!!Warning!!! Some values are overriden at the end of the file." > ${CONFIG_FILE}
echo >> ${CONFIG_FILE}
cat ${KITE_INSTALLATION_BASE}/conf/kiterc_template >> ${CONFIG_FILE}
echo >> ${CONFIG_FILE}
echo >> ${CONFIG_FILE}
echo "# Override settings created by start_ec2_cluster.sh. " >> ${CONFIG_FILE}
echo "# These will reset some values above. Feel free to edit as necessary." >> ${CONFIG_FILE}
echo 'export SPARK_HOME=/root/spark' >> ${CONFIG_FILE}
echo 'export SPARK_MASTER="spark://`curl http://169.254.169.254/latest/meta-data/public-hostname`:7077"' >> ${CONFIG_FILE}
echo "export KITE_DATA_DIR=s3n://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}@${S3_DATAREPO}" >> ${CONFIG_FILE}
echo "export SPARK_EXECUTOR_MEMORY=$((RAM_GB - 2))g" >> ${CONFIG_FILE}
echo "export KITE_MASTER_MEMORY_MB=$((1024 * (RAM_GB - 4)))" >> ${CONFIG_FILE}
echo "export KITE_HTTP_PORT=5080" >> ${CONFIG_FILE}
echo "export KITE_LOCAL_TMP=${LOCAL_TMP_DIR}" >> ${CONFIG_FILE}

HOST=`aws ec2 describe-instances --region=${REGION} --filters "Name=instance.group-name,Values=${CLUSTER_NAME}-master" | grep PublicDnsName | grep ec2 | cut -d'"' -f 4 | head -1`

SSH="ssh -i $HOME/.ssh/${SSH_KEY} -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no"
rsync -ave "$SSH" ${CONFIG_FILE} root@${HOST}:.kiterc
