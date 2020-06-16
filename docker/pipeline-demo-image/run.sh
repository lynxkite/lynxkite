#!/bin/bash -xue
# Runs the singtel-lk-demo image.
# Put your dags into the dags directory here
# for Airflow to see them.

cd $(dirname $0)
DIR=`pwd`
mkdir -p dags
docker run --rm -d \
       -p 2200:2200 \
       -p 8080:8080 \
       -p 9596:9596 \
       -p 5432:5432 \
       -p 9001:9001 \
       -v $DIR/dags:/tmp/mount/dags \
       pipeline-demo
