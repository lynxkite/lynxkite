#!/bin/bash -xue
# Run Python API tests. You can start LynxKite manually, or use tools/with_lk.sh.

BASEDIR=$(git rev-parse --show-toplevel)
cd $(dirname $0)

if [ -n "${HTTPS_PORT:-}" ]; then
  export LYNXKITE_ADDRESS="https://localhost:$HTTPS_PORT/"
  export LYNXKITE_PUBLIC_SSL_CERT="$BASEDIR/test/localhost.self-signed.cert.pub"
  export LYNXKITE_USERNAME="admin"
  export LYNXKITE_PASSWORD="adminpw"
else
  export LYNXKITE_ADDRESS="http://localhost:${PORT:-2200}/"
fi

export PYTHONPATH=src
python3 -m mypy src/lynx --ignore-missing-import
python3 -m unittest discover tests $@
