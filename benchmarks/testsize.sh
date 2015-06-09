#!/bin/bash

# Script to test the effect of changing maximal allowed partition size relative to memory per core.

GLOB=$1

DIR=$(dirname $0)
cd $DIR/..

RATIO=0.050625
for i in `seq 10`; do
rm -r ~/kite_meta
rm -r ~/kite_data/entities
rm -r ~/kite_data/scalars
WORKER_MEMORY_MULT=$RATIO time ./stage/bin/biggraph batch kitescripts/importtest glob:${GLOB}
RATIO=`echo "scale=10; $RATIO * 1.5" | bc`
done
