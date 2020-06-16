#!/bin/bash -xue

cd $(dirname $0)

docker build -t bus-pipeline-demo .
