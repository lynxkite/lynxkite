#!/bin/bash -xue
# Tests the maximal length of the post message that LynxKite can handle.
#
# Usage: ./post_test.sh <length>
#
# where <length> is roughly the number of bytes we want to send in the post message.
#
# E.g., ./post_test 1000000
#

TMP=`mktemp`
Restore() {
    rm -rf $TMP
}

LEN="$1"
RND=`python3 -c "import os;print(os.urandom(16).hex())"`
WORKSPACE="w${RND}"

trap Restore EXIT
python3 - << EOF > $TMP
import json
import sys

l = "$LEN"
x = "x"*int(l)
x = f'/* {x} */ 1.0'
j = {
  "reference": {
    "top": "$WORKSPACE",
    "customBoxStack": []
  },
  "workspace": {
    "boxes": [
      {
        "id": "anchor",
        "operationId": "Anchor",
        "parameters": {},
        "x": 0,
        "y": 0,
        "inputs": {},
        "parametricParameters": {}
      },
      {
        "id": "Create-example-graph_1",
        "operationId": "Create example graph",
        "parameters": {},
        "x": 242,
        "y": 204,
        "inputs": {},
        "parametricParameters": {}
      },
      {
        "id": "Derive-vertex-attribute_1",
        "operationId": "Derive vertex attribute",
        "parameters": {
          "output": "name",
          "expr": x
        },
        "x": 437,
        "y": 232,
        "inputs": {
          "project": {
            "boxId": "Create-example-graph_1",
            "id": "project"
          }
        },
        "parametricParameters": {}
      }
    ]
  }
}
json.dump(j, sys.stdout)

EOF

curl -H "Content-type: application/json" -d "{\"name\": \"$WORKSPACE\"}" -X POST http://localhost:2200/ajax/createWorkspace
curl -H "Content-type: application/json" -v --data  @${TMP} http://localhost:2200/ajax/setAndGetWorkspace
