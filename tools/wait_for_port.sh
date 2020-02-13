#!/usr/bin/env bash
# Wait until the specified port is up.

PORT=$1
HOST=${WAIT_FOR_HOST:-localhost}

while ! echo > /dev/tcp/${HOST}/${PORT}; do
  sleep 1
done
