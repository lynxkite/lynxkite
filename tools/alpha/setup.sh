#!/bin/sh -xue

# Make sure we are not killing someone's laptop.
if [ $(whoami) != 'ec2-user' ]; then
  echo This script must be run on the EC2 instance.
  exit 1
fi

# Mount the SSD if it's not mounted yet.
if [ ! -e /media/ephemeral0/lost+found ]; then
  sudo mkfs.ext4 /dev/xvdb
  sudo mount /media/ephemeral0
  sudo chown ec2-user /media/ephemeral0
fi

# Redirect port 80 to port 9000.
sudo iptables -A PREROUTING -t nat -i eth0 -p tcp --dport 80 -j REDIRECT --to-port 9000

CREDENTIALS=$1
CORES=4
RAM_MB=28000

# Stop the server in case it's already running.
killall java || true
for i in $(seq 10); do
  if [ ! -e biggraphstage/RUNNING_PID ]; then
    break
  fi
  sleep 1
done
if [ -e biggraphstage/RUNNING_PID ]; then
  killall -9 java
  rm -f biggraphstage/RUNNING_PID
fi

# Start the server.
sh -c "( ( \
  NUM_CORES_PER_EXECUTOR=${CORES} \
  REPOSITORY_MODE=\"static</home/ec2-user/metagraph,s3n://${CREDENTIALS}@lynx-bnw-data>\" \
  SPARK_CLUSTER_MODE=\"static<local[${CORES}]>\" \
  SPARK_JAVA_OPTS=\"-Dhadoop.tmp.dir=/media/ephemeral0/hadoop-tmp\" \
  SPARK_DIR=\"/media/ephemeral0/\" \
  EXECUTOR_MEMORY=${RAM_MB}m \
  nohup biggraphstage/bin/biggraph \
    -mem $RAM_MB \
  &> setup.sh.out \
) & ls > /dev/null )"
# I have no idea why, but if I remove "ls", nohup does not work.
