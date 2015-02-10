#!/bin/bash -xue

DIR=$(dirname $0)
source $DIR/settings.sh
if [ "${CREDENTIALS:-}" = "" ]; then
  echo "CREDENTIALS has to be set to <key name>:<secret key>"
  exit 1
fi
if [ "${GOOGLE_CLIENT_SECRET:-}" = "" ]; then
  echo "GOOGLE_CLIENT_SECRET has to be set"
  echo "You can find it at https://console.developers.google.com/project/apps~big-graph-gc1/apiui/credential"
  exit 1
fi

echo "Staging..."
SSH="ssh -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no -i $SSH_KEY"
rsync -ave "$SSH" -r --copy-dirlinks tools/ ec2-user@$ELASTIC_IP:biggraphstage
REMOTE_HOST=ec2-user@$ELASTIC_IP $DIR/../../remote_stage.sh -i "$SSH_KEY"

echo "Starting..."
ssh \
  -i "$SSH_KEY" \
  -o UserKnownHostsFile=/dev/null \
  -o CheckHostIP=no \
  -o StrictHostKeyChecking=no \
  -t -t \
  ec2-user@$ELASTIC_IP \
  biggraphstage/alpha/setup.sh $CREDENTIALS $GOOGLE_CLIENT_SECRET "$@"
