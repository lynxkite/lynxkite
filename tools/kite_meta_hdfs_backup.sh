# Add the script to the crontab e.g. like this:
# 59 22 * * * $LOCATION_TO_SCRIPT
# Make sure you edit the crontab with the same user which has Kite.

set -eu

BASEDIR=$(dirname $0)
# .kiterc needs SPARK_VERSION
SPARK_VERSION=$(cat "$BASEDIR/../conf/SPARK_VERSION")
USER=$(whoami)
KITERC_FILE=$HOME/.kiterc

# Loading $KITE_META_DIR from .kiterc.
source $KITERC_FILE
KITE_META=$(basename $KITE_META_DIR)
TMP_BACKUP_DIR=$KITE_LOCAL_TMP/kite_meta_backup
HDFS_BACKUP_DIR=/user/$USER/backup

# Copy the kite meta dir to a temporary location for cleansing.
rm -r $TMP_BACKUP_DIR
mkdir $TMP_BACKUP_DIR
cp -r $KITE_META_DIR $TMP_BACKUP_DIR/

# Remove unnecessary lines from tags.journal to save space.
for FILE in $TMP_BACKUP_DIR/$KITE_META/*/tags.journal
do
  grep -v '/!tmp' $FILE > $TMP_BACKUP_DIR/tags.journal.tmp
  mv $TMP_BACKUP_DIR/tags.journal.tmp $FILE
done

tar -czf $TMP_BACKUP_DIR/kite_meta.tgz $TMP_BACKUP_DIR/$KITE_META

# Copy a backup to HDFS.
CURRENT_DATE=$(date +"%y%m%d")
HDFS_BACKUP_DIR=$HDFS_BACKUP_DIR/$CURRENT_DATE
hadoop fs -mkdir -p $HDFS_BACKUP_DIR
hadoop fs -put  $KITERC_FILE $HDFS_BACKUP_DIR/
hadoop fs -put  $TMP_BACKUP_DIR/kite_meta.tgz $HDFS_BACKUP_DIR/

