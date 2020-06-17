#!/bin/bash -xue

cd $(dirname $0)
gcloud container clusters get-credentials us-flex --zone us-central1-a --project external-lynxkite
helm upgrade cloud-lk jupyterhub/jupyterhub \
  --install --debug --version=0.9-dcde99a --namespace=cloud-lk --force \
  -f config.yml -f config-credentials.yml
