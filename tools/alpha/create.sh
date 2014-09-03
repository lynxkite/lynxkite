#!/bin/sh -xue

DIR=$(dirname $0)
source $DIR/settings.sh

INSTANCE=$(\
aws ec2 run-instances \
  --image-id $AMI \
  --count 1 \
  --instance-type $TYPE \
  --key-name lynx-cli \
  --security-groups alpha \
  --block-device-mappings '[{"DeviceName":"/dev/xvdb","VirtualName":"ephemeral0"}]' \
| grep INSTANCES | cut -f 8)

echo "Associating IP address $ELASTIC_IP with instance $INSTANCE"
set -e
while true; do
  aws ec2 associate-address --instance-id $INSTANCE --public-ip $ELASTIC_IP && break
done
set +e

$DIR/update.sh

echo "Instance coming up at http://$ELASTIC_IP/"
