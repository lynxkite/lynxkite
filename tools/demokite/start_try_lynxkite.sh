#!/bin/bash -xue

gcloud config set project external-lynxkite
export CONTINENT='us'
export REGION='us-central1'
export ZONE='us-central1-a'
export MACHINE_TYPE='n1-highcpu-16'

cd $(dirname $0)
./start.sh try-lynxkite-us
