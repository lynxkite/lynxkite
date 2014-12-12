#!/bin/sh -xue

ROOT=$(dirname $0)
SPARK_MASTER="spark://`curl http://169.254.169.254/latest/meta-data/public-hostname`:7077"
CREDENTIALS=$1
CORES=$2
RAM_MB=$3
EXECUTOR_MB=$4
S3_DATAREPO=$5
shift 5
EXTRA_ARGS="$@"

# Stop the server in case it's already running.
if [ -e biggraphstage/RUNNING_PID ]; then
  PID=$(cat ${ROOT}/RUNNING_PID)
  kill $PID || true
  for i in $(seq 10); do
    if [ ! -e /proc/$PID ]; then
      break
    fi
    sleep 1
  done
  if [ -e /proc/$PID ]; then
    kill -9 $PID || true
    rm -f biggraphstage/RUNNING_PID
  fi
fi

# Start the server.
sh -c "( ( \
  NUM_CORES_PER_EXECUTOR=${CORES} \
  REPOSITORY_MODE=\"static</home/ec2-user/metagraph,s3n://${CREDENTIALS}@${S3_DATAREPO}>\" \
  SPARK_CLUSTER_MODE=\"static<${SPARK_MASTER}>\" \
  SPARK_JAVA_OPTS=\"-Dhadoop.tmp.dir=/mnt/hadoop-tmp\" \
  SPARK_DIR=\"/mnt/\" \
  EXECUTOR_MEMORY=${EXECUTOR_MB}m \
  LOGGER_HOME=${LOGGER_HOME:-/mnt} \
  SPARK_HOME=${SPARK_HOME:-/home/ec2-user/spark-1.1.0-bin-hadoop1} \
  nohup $ROOT/bin/biggraph \
    -mem $RAM_MB \
    -Dhttp.port=5080 \
    $EXTRA_ARGS \
  &> run_on_ec2.sh.out \
) & ls > /dev/null )"
# I have no idea why, but if I remove "ls", nohup does not work.
