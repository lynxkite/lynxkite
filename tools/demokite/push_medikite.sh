#!/bin/bash -xue

cd $(dirname $0)
export KITE_TITLE="LynxKite"
export KITE_TAGLINE="The Complete Graph Data Science Platform"
export CONTINENT='eu'
export REGION='europe-west3'
export ZONE='europe-west3-c'
export PROJECT='external-lynxkite'
export KITE_HOSTNAME='health-radar.lynxkite.com'
export KITE_MASTER_MEMORY_MB=6000
export SPHYNX_CACHED_ENTITIES_MAX_MEM_MB=6000
export KITE_ALLOW_PYTHON='yes'
./push.sh medikite "$@"
