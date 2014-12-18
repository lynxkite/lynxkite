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

# Redirect port 80 to port 9000, 443 to 9001.
sudo iptables -A PREROUTING -t nat -i eth0 -p tcp --dport 80 -j REDIRECT --to-port 9000
sudo iptables -A PREROUTING -t nat -i eth0 -p tcp --dport 443 -j REDIRECT --to-port 9001

CREDENTIALS=$1
shift
GOOGLE_CLIENT_SECRET=$1
shift
CORES=4
RAM_MB=28000

# Create script for running the server.
EXTRA_ARGS="$@"
cat > run.sh <<EOF
#!/bin/sh -xue
# Stop the server in case it's already running.
if [ -e biggraphstage/RUNNING_PID ]; then
  PID=\$(cat biggraphstage/RUNNING_PID)
  kill \$PID || true
  for i in \$(seq 10); do
    if [ ! -e /proc/\$PID ]; then
      break
    fi
    sleep 1
  done
  if [ -e /proc/\$PID ]; then
    kill -9 \$PID || true
    rm -f biggraphstage/RUNNING_PID
  fi
fi

# Start it up.
export NUM_CORES_PER_EXECUTOR=${CORES}
export REPOSITORY_MODE="static</home/ec2-user/metagraph,s3n://${CREDENTIALS}@lynx-bnw-data>"
export SPARK_MASTER="local[${CORES}]"
export SPARK_DIR="/media/ephemeral0/"
export SPARK_HOME=/home/ec2-user/spark-1.1.0-bin-hadoop1
biggraphstage/bin/biggraph \
  -mem $RAM_MB \
  -Dapplication.secret=$CREDENTIALS \
  -Dauthentication.google.clientSecret=$GOOGLE_CLIENT_SECRET \
  -Dhadoop.tmp.dir=/media/ephemeral0/hadoop-tmp \
  $EXTRA_ARGS
EOF
chmod +x run.sh

# Start server.
biggraphstage/alpha/detached_run.sh ./run.sh

# Start watchdog.
killall -9 python || true
biggraphstage/alpha/detached_run.sh " \
  biggraphstage/watchdog.py \
    --status_port=9999 \
    --watched_url=http://pizzakite.lynxanalytics.com/ \
    --sleep_seconds=10 \
    --max_failures=10 \
    --script='/home/ec2-user/biggraphstage/alpha/detached_run.sh /home/ec2-user/run.sh' \
  "
