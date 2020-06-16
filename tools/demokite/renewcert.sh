#!/bin/bash -xue
# Renews the SSL certificate. Run this twice a day with a cronjob.
# LynxKite must be running for this to work. It will restart LynxKite if a new certificate is
# created.

cd $(dirname $0)

HOST_NAME=$1

gethash() {
  sudo bash -c "cat /etc/letsencrypt/live/$HOST_NAME/* | md5sum"
}
BEFORE=$(gethash)

sudo letsencrypt certonly \
  --agree-tos \
  --email administrator@lynxanalytics.com \
  --keep \
  --webroot \
  -w kite/static \
  -d $HOST_NAME

AFTER=$(gethash)
# Restart LynxKite only if the certificate has changed.
if [ "$BEFORE" != "$AFTER" ]; then
  docker restart lynxkite
fi
