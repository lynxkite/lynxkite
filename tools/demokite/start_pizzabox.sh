#!/bin/bash -xue

export CONTINENT='asia'
export REGION='asia-east1'
export ZONE='asia-east1-a'

cd $(dirname $0)
./start.sh pizzabox
