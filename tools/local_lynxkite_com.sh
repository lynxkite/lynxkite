#!/bin/bash -xue
# Requests a Let's Encrypt certificate for "local.lynxkite.com".
# This involves starting up and shutting down a tiny GCE instance.

INSTANCE="${USER}-letsencrypt"
ZONE='europe-west1-c'

gcloud compute instances create \
  $INSTANCE \
  --address 'local-lynxkite-com' \
  --project 'big-graph-gc1' \
  --image-project 'ubuntu-os-cloud' \
  --image-family 'ubuntu-1604-lts' \
  --machine-type 'f1-micro' \
  --tags 'http-server' \
  --zone $ZONE

SSH="gcloud compute ssh $INSTANCE --zone $ZONE --command"
# Wait for SSH. (It times out for the first try.)
while [[ $($SSH "echo hello") != "hello" ]]; do
  sleep 1
done
$SSH "sudo apt-get install -y letsencrypt"
$SSH \
  "sudo letsencrypt certonly \
    --non-interactive \
    --standalone \
    --domain 'local.lynxkite.com' \
    --email 'administrator@lynxanalytics.com' \
    --agree-tos"
$SSH "sudo chmod -R 755 /etc/letsencrypt"

gcloud compute copy-files \
  "$INSTANCE:/etc/letsencrypt/live/local.lynxkite.com" \
  '/tmp/local.lynxkite.com' \
  --zone $ZONE
gcloud compute instances delete \
  --quiet \
  $INSTANCE \
  --zone $ZONE

cd $(dirname $0)/../ecosystem/native/glue/config/ssl/
DEST=$(pwd)
cd '/tmp/local.lynxkite.com'
openssl \
  pkcs12 \
  -export \
  -in 'fullchain.pem' \
  -inkey 'privkey.pem' \
  -out 'local.lynxkite.com.cert' \
  -name 'tomcat' \
  -CAfile 'chain.pem' \
  -caname 'root' \
  -password 'pass:keystore-password'
mv 'local.lynxkite.com.cert' $DEST
rm -rf '/tmp/local.lynxkite.com'
