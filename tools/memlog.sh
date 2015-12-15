# A simple tool to torce GC and log JVM's heap usage periodically.
# Only use with Oracle Java 8.
# https://docs.oracle.com/javase/8/docs/technotes/tools/unix/jstat.html

if [ $# -lt 4 ]; then
    echo "Usage: memlog.sh PID OUTPUT_FILE TIME_INTERVAL_SEC PROCESS_OWNER"
    exit 1
fi

PID=$1
OUTPUT_FILE=$2
TIME_INTERVAL_SEC=$3
PROCESS_OWNER=$4

rm -f $OUTPUT_FILE
echo "Eden space utilization;Old space utilization" >> $OUTPUT_FILE

while :
do
   sudo -u $PROCESS_OWNER jcmd $PID GC.run
   sudo -u $PROCESS_OWNER jstat -gc $PID | \
     tail -1 | \ # No need for headers
     awk '{print $6 ";" $8}' >> # Java 8 jstat prints Eden and Old space util as the 6th and 8th col
     $OUTPUT_FILE
   sleep $TIME_INTERVAL_SEC
done
