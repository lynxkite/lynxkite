#!/bin/bash
#
# Runs the bigraph uploadLogs, which uploads the logs and operation performance data
# to google storage.
# This script should be run as a cron job!
# Copy this file to /etc/cron.monthly/ for automatic monthly uploads.

# Set this variable to the biggraph script log file. (The same as KITE_SCRIPT_LOGS)
SCRIPTLOGS=${HOME}/biggraph.log

checkFile() {
    FILE=$1
    if [ ! -e $FILE ]; then
        >&2 echo "File '$FILE' does not exist!"
        exit 1
    fi

    if [ ! -f $FILE ]; then
        >&2 echo "File '$FILE' is not a regular file!"
        exit 1
    fi

    if [ ! -r $FILE ]; then
        >&2 echo "File '$FILE' cannot be read!"
        exit 1
    fi
}

checkFile $SCRIPTLOGS

COMMAND=`tail -n1 $SCRIPTLOGS | cut -d ' ' -f3`

if [[ ! $COMMAND =~ biggraph$ ]]; then
    >&2 echo "The command: '$COMMAND' doesn't look like a bigraph script"
fi

checkFile $COMMAND

if [ ! -x $COMMAND ]; then
    >&2 echo "File $COMMAND is not executable"
fi

$COMMAND uploadLogs

