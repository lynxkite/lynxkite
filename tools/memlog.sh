# A simple tool to torce GC and log JVM's heap usage periodically.
# Only use with Oracle Java 8.
# https://docs.oracle.com/javase/8/docs/technotes/tools/unix/jstat.html
# Note that the JVM process (in case of a Hadoop deployment the NodeManager)
# needs to be launched with the -XX:+StartAttachListener Java config option.

set -eu
set -o pipefail

if [ $# -lt 4 ]; then
    echo "Usage: memlog.sh PROCESS_ID PROCESS_OWNER OUTPUT_FILE TIME_INTERVAL_SEC"
    exit 1
fi

PROCESS_ID=$1
PROCESS_OWNER=$2
OUTPUT_FILE=$3
TIME_INTERVAL_SEC=$4

rm -f $OUTPUT_FILE
echo "Eden space utilization;Old space utilization" >> $OUTPUT_FILE

while :
do
   sudo -u $PROCESS_OWNER jcmd $PROCESS_ID GC.run
   sudo -u $PROCESS_OWNER jstat -gc $PROCESS_ID | \
     tail -1 | \ # No need for headers
     awk '{print $6 ";" $8}' >> # Java 8 jstat prints Eden and Old space util as the 6th and 8th col
     $OUTPUT_FILE
   sleep $TIME_INTERVAL_SEC
done
