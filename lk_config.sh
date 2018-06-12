#!/bin/bash -xue

if [ -n "${HTTPS_PORT:-}" ]; then
  export LYNXKITE_ADDRESS="https://localhost:$HTTPS_PORT/"
  export LYNXKITE_PUBLIC_SSL_CERT="$BASEDIR/test/localhost.self-signed.cert.pub"
  export LYNXKITE_USERNAME="admin"
  export LYNXKITE_PASSWORD="adminpw"
else
  export LYNXKITE_ADDRESS="http://localhost:${PORT:-2200}/"
fi
