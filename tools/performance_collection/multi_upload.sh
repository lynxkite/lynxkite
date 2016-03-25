#!/bin/bash
#
#  This script uploads kite logfiles and the operation performance data to the google cloud storage.
#
#  Usage: multi_upload.sh PORT LOGDIR INSTANCE
# 
#  PORT: The port of the running LynxKite or 0, if there's no running LynxKite
#
#  LOGDIR: The directory containing the logs to be uploaded
#
#  INSTANCE: The name of the instance
#
#  Example:
#
#  1) Upload the logs of the running pizzakite (assuming port 2200)
#  stage/tools/performace_collection/multi_upload.sh 2200 stage/logs pizzakite
#
#  2) The same as above, but don't ask the running kite to rotate the logs:
#  stage/tools/performace_collection/multi_upload.sh 0 stage/logs pizzakite
#

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 kiteport logdirectory instance" 1>&2
    exit 1
fi


PORT=$1
LOGDIR=$2
INSTANCE=$3

TMPDIR=`mktemp -d`


FILELIST=${TMPDIR}/filelist
LOGCOPY=${TMPDIR}/logcopy

CMDDIR="$(dirname $0)"

ls ${LOGDIR}/application-*.log > $FILELIST

# Force log rotation of the running app.
if [ "$PORT" -ne "0" ]; then
    curl -d '{"fake": 0}' -H "Content-Type: application/json" http://localhost:${PORT}/forceLogRotate
    sleep 2
fi

cat $FILELIST | while read logfile; do
    grep "^UPLOADED$" -q $logfile
    if [ "$?" -ne "0" ]; then
        echo "Uploading $logfile"
        NAME=`basename $logfile | sed 's/[.]log$//'`        
        OPLOGNAME=ops-${INSTANCE}-${NAME}.txt        
        ${CMDDIR}/parse_log.sh $logfile $TMPDIR/$OPLOGNAME
        ${CMDDIR}/gupload.sh log $logfile ${INSTANCE}/${NAME}.log
        if [ -s ${TMPDIR}/${OPLOGNAME} ]; then
            ${CMDDIR}/gupload.sh ops ${TMPDIR}/${OPLOGNAME} ${OPLOGNAME}
        else
            echo "No performance data found in $logfile"
        fi
        echo "UPLOADED" >> $logfile        
    else
        echo "File $logfile already uploaded - skipping"
    fi
done



rm -r $TMPDIR

