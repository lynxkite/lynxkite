#!/usr/bin/env bash
# Wait until the specified port is up.

PORT=$1

sleep 2
while ! nc -z localhost ${PORT}; do
  sleep 1
done
