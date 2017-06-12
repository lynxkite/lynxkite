#!/usr/bin/env bash
# Wait until the specified port is up.

PORT=$1

while ! nc -z -w 1 localhost ${PORT}; do
  sleep 1
done
