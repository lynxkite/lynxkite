#!/bin/bash

# Reads a kite log file and extracts the json performance data created by OperationLogger

if [[ "$#" != "2" ]]; then
    echo "Usage: $0 logfile output" 2>&1
    exit 1
fi

LOGFILE="$1"
OUTPUT="$2"
cat "$LOGFILE" | grep OPERATION_LOGGER_MARKER | sed 's/^.*OPERATION_LOGGER_MARKER //' > "$OUTPUT"

