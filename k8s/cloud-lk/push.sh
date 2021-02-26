#!/bin/bash -xue

cd $(dirname $0)
gcloud container clusters get-credentials us-flex --zone us-central1-a --project external-lynxkite
helm upgrade cloud-lk jupyterhub/jupyterhub \
  --install --debug --version=0.11.1 --namespace=cloud-lk \
  -f config.yml -f config-credentials.yml
