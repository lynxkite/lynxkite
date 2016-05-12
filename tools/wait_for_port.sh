#!/usr/bin/env bash
# Wait until the specified port is up (but for at most 30 seconds)

PORT=$1

START=`date +"%s"`
EXPIRES=$((START + 30))
while ! nc -z localhost ${PORT};
do
    sleep 1;
    NOW=`date +"%s"`
    if [ "$NOW" -gt "$EXPIRES" ]; then
        echo "Timeout while waiting for port $PORT" 1>&2
        exit 1
    fi
done
