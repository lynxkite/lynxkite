# A simple tool to torce GC and log JVM's heap usage periodically.
# Only use with Oracle Java 8.
# https://docs.oracle.com/javase/8/docs/technotes/tools/unix/jstat.html

set -eu
set -o pipefail

if [ $# -lt 2 ]; then
    echo "Usage: memlog.sh PROCESS_ID USER [TIME_INTERVAL_SEC]"
    exit 1
fi

PROCESS_ID=$1
USER=$2
TIME_INTERVAL_SEC=10
if [ $# -eq 3 ]; then
  TIME_INTERVAL_SEC=$3    
fi

echo -e "Eden space utilization\tOld space utilization"

while :
do
   sudo -u $USER jcmd $PROCESS_ID GC.run 1>/dev/null
   # Skip header and ignore the unimportant columns
   sudo -u $USER jstat -gc $PROCESS_ID | \
     tail -1 | awk '{print $6 "\t" $8}'
   sleep $TIME_INTERVAL_SEC
done
