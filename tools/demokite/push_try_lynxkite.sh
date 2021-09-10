#!/bin/bash -xue

cd $(dirname $0)
export KITE_TITLE="LynxKite"
export KITE_TAGLINE="The Complete Graph Data Science Platform"
export KITE_GOOGLE_CLIENT_ID='422846954881-rn42akejs9ik195i28p2v7u872cq3v43.apps.googleusercontent.com'
export KITE_GOOGLE_PUBLIC_ACCESS='yes'
export KITE_GOOGLE_WIZARD_ONLY='yes'
export KITE_ACCESS_WITHOUT_LOGIN='yes'
export KITE_HOME_WITHOUT_LOGIN='custom_boxes/hidden'
export KITE_WIZARD_ONLY_WITHOUT_LOGIN='yes'
export CONTINENT='us'
export REGION='us-central1'
export ZONE='us-central1-a'
export PROJECT='external-lynxkite'
export KITE_HOSTNAME='try.lynxkite.com'
export KITE_MASTER_MEMORY_MB=6000
export NUM_CORES_PER_EXECUTOR=8
export SPHYNX_CACHED_ENTITIES_MAX_MEM_MB=6000
export KITE_ALLOW_PYTHON='yes'
# We need to pass it through Bash and JSON. It's easier to load from a file than to manually escape.
export KITE_FRONTEND_CONFIG=$(python -c '
import json
import sys
config = { "defaultFolder": "", "folderDescriptions": { "": sys.stdin.read() } }
print(json.dumps(config))
' < try-lynxkite-com-welcome.md)
./push.sh try-lynxkite-us "$@"
