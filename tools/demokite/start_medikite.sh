#!/bin/bash -xue

gcloud config set project external-lynxkite
export CONTINENT='eu'
export REGION='europe-west3'
export ZONE='europe-west3-c'

cd $(dirname $0)
./start.sh medikite
