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
echo -e "Eden space utilization\tOld space utilization" >> $OUTPUT_FILE

while :
do
   sudo -u $PROCESS_OWNER jcmd $PROCESS_ID GC.run
   # Skip header and ignore the unimportant columns
   sudo -u $PROCESS_OWNER jstat -gc $PROCESS_ID | \
     tail -1 | awk '{print $6 "\t" $8}' >> $OUTPUT_FILE
   sleep $TIME_INTERVAL_SEC
done
