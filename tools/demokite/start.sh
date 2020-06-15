#!/bin/bash -xue
# Starts a GCE instance for hosting LynxKite.

INSTANCE=$1

AddressExists() {
  gcloud compute addresses describe $INSTANCE --region $REGION
}

if ! AddressExists; then
  gcloud compute addresses create $INSTANCE --region $REGION
fi

MACHINE_TYPE=${MACHINE_TYPE:-'n1-standard-8'}

gcloud compute instances create \
  $INSTANCE \
  --address $INSTANCE \
  --project 'big-graph-gc1' \
  --boot-disk-size '1TB' \
  --image-project 'ubuntu-os-cloud' \
  --image-family 'ubuntu-1604-lts' \
  --machine-type ${MACHINE_TYPE} \
  --tags 'persistent,http-server' \
  --zone ${ZONE}

# Install Docker.
SSH="gcloud compute ssh $INSTANCE --zone ${ZONE} --command"
# Wait for SSH. (It times out for the first try.)
while [[ $($SSH "echo hello") != "hello" ]]; do
  sleep 1
done
$SSH "sudo adduser --disabled-password -gecos 'Service account' kite"
$SSH "sudo apt-get update"
$SSH "sudo apt-get install -y letsencrypt"
$SSH "sudo bash -c 'curl -sSL https://get.docker.com/ | sh'"
$SSH "sudo usermod -aG docker kite"
$SSH "sudo usermod -aG sudo kite"
$SSH "sudo sed -i 's/%sudo\s\+ALL=(ALL:ALL)\s\+ALL/%sudo  ALL=(ALL:ALL) NOPASSWD: ALL/' /etc/sudoers"
