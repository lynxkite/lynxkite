#!/bin/sh -xue

ROOT=$(dirname $0)
CORES=4
RAM_MB=28000
SPARK_MASTER="spark://`curl http://instance-data.ec2.internal/latest/meta-data/public-hostname`:7077"
CREDENTIALS=$1
shift
EXTRA_ARGS="$@"

# Stop the server in case it's already running.
if [ -e ${ROOT}/RUNNING_PID ]; then
  kill `cat ${ROOT}/RUNNING_PID`
  sleep 2
fi
if [ -e ${ROOT}/RUNNING_PID ]; then
  killall -9 `cat ${ROOT}/RUNNING_PID`
  rm -f ${ROOT}/RUNNING_PID
fi

# Start the server.
sh -c "( ( \
  NUM_CORES_PER_EXECUTOR=${CORES} \
  REPOSITORY_MODE=\"static</home/ec2-user/metagraph,s3n://${CREDENTIALS}@lynx-bnw-data>\" \
  SPARK_CLUSTER_MODE=\"static<${SPARK_MASTER}>\" \
  SPARK_JAVA_OPTS=\"-Dhadoop.tmp.dir=/media/ephemeral0/hadoop-tmp\" \
  SPARK_DIR=\"/media/ephemeral0/\" \
  EXECUTOR_MEMORY=${RAM_MB}m \
  nohup $ROOT/bin/biggraph \
    -mem $RAM_MB \
    -Dhttp.port=5080 \
    $EXTRA_ARGS \
  &> run_on_ec2.sh.out \
) & ls > /dev/null )"
# I have no idea why, but if I remove "ls", nohup does not work.
