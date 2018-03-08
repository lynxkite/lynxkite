#!/bin/bash -xue
# Run Python API tests. You can start LynxKite manually, or use tools/with_lk.sh.

if [ -n "${HTTPS_PORT:-}" ]; then
  export LYNXKITE_ADDRESS="https://localhost:$HTTPS_PORT/"
  export LYNXKITE_PUBLIC_SSL_CERT="../../test/localhost.self-signed.cert.pub"
  export LYNXKITE_USERNAME="admin"
  export LYNXKITE_PASSWORD="adminpw"
else
  export LYNXKITE_ADDRESS="http://localhost:${PORT:-2200}/"
fi

cd $(dirname $0)
mypy lynx --ignore-missing-import
python3 -m unittest discover tests $@
