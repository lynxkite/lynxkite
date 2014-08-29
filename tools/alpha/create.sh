#!/bin/sh -xue

SSH_KEY=${SSH_KEY:-~/.ssh/lynx-cli.pem}
if [ "$CREDENTIALS" == "" ]; then
  echo CREDENTIALS has to be set to <key name>:<secret key>
  exit 1
fi
DIR=$(dirname $0)
# Amazon Linux AMI 2014.03.2 (HVM)
AMI=ami-76817c1e
# 4 cores, 30GB RAM, 1x80GB SSD, $0.35 per hour
TYPE=r3.xlarge

INSTANCE=$(\
aws ec2 run-instances \
  --image-id $AMI \
  --count 1 \
  --instance-type $TYPE \
  --key-name lynx-cli \
  --security-groups alpha \
  --block-device-mappings '[{"DeviceName":"/dev/xvdb","VirtualName":"ephemeral0"}]' \
| grep INSTANCES | cut -f 8)

ELASTIC_IP=54.83.18.233
echo "Associating IP address $ELASTIC_IP with instance $INSTANCE"
set -e
while true; do
  aws ec2 associate-address --instance-id $INSTANCE --public-ip $ELASTIC_IP && break
done
set +e

echo "Staging..."
REMOTE_HOST=ec2-user@$ELASTIC_IP $DIR/../../remote_stage.sh -i "$SSH_KEY"

echo "Starting..."
ssh \
  -i "$SSH_KEY" \
  -o UserKnownHostsFile=/dev/null \
  -o CheckHostIP=no \
  -o StrictHostKeyChecking=no \
  -t -t \
  ec2-user@$ELASTIC_IP \
  biggraphstage/alpha/setup.sh $CREDENTIALS

echo "Instance coming up at http://$ELASTIC_IP/"
