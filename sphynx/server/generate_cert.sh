#!/bin/bash -xue
# Generates certificate for the gRPC server.

openssl req -x509 -sha256 -newkey rsa:4096 -keyout private-key.pem -out cert.pem -days 365 -nodes \
  -subj "/C=/ST=/L=/O=Lynx Analytics/OU=Org/CN=sphynx"

cp cert.pem ../../app/com/lynxanalytics/biggraph/graph_api/
