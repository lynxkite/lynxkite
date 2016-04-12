# ## Automatic log collection

# For Internet-connected LynxKite instances, it is possible to set up a cronjob that uploads the logs created
# by the running instance to Google storage. This is useful partly for debugging purposes, partly for
# maintaining a database that can answer questions such as how much time does
# it usually take to perform this and that operation.

# Note that (1) the logs will be uploaded to a South Asia region,
# and (2) logs already uploaded will not be uploaded again.

# To configure this, copy the following script (which can be found at /tools/cron_upload.sh)
# to /etc/cron.monthly; this way uploading will be triggered in every month.
# It is, of course, possible to create a custom setup; monthly upload is just a
# recommended setting. You might need to edit the script to ensure that
# the SCRIPTLOGS variable equals KITE_SCRIPT_LOGS (see <<kiterc-script-logs>>).
# Please, also make sure that KITE_INSTANCE (<<kite-instance>>) is set in .kiterc,
# otherwise log upload will not work.

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

