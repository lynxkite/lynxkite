# Add the script to the crontab e.g. like this:
# 59 22 * * * $LOCATION_TO_SCRIPT
# Make sure you edit the crontab with the same user which has Kite.

set -e

export USER=$(whoami)
export KITERC_FILE=$HOME/.kiterc

# Loading $KITE_META_DIR from .kiterc.
source $KITERC_FILE
export KITE_META=$(basename $KITE_META_DIR)
export TMP_BACKUP_DIR=$KITE_LOCAL_TMP/kite_meta_backup
export HDFS_BACKUP_DIR=/user/$USER/backup

# Copy the kite meta dir to a temporary location for cleansing.
rm -r $TMP_BACKUP_DIR
mkdir $TMP_BACKUP_DIR
cp -r $KITE_META_DIR $TMP_BACKUP_DIR/

# Remove unnecessary lines from tags.journal to save space.
for FILE in $TMP_BACKUP_DIR/$KITE_META/*/tags.journal
do
  grep -v '/!tmp' $FILE > $TMP_BACKUP_DIR/tags.journal.tmp
  cp $TMP_BACKUP_DIR/tags.journal.tmp $FILE
  rm $TMP_BACKUP_DIR/tags.journal.tmp 
done

tar -czf $TMP_BACKUP_DIR/kite_meta.tgz $TMP_BACKUP_DIR/$KITE_META

# Copy a backup to HDFS.
export CURRENT_DATE=$(date +"%y%m%d")
export HDFS_BACKUP_DIR=$HDFS_BACKUP_DIR/$CURRENT_DATE
hadoop fs -mkdir -p $HDFS_BACKUP_DIR
hadoop fs -put  $KITERC_FILE $HDFS_BACKUP_DIR/
hadoop fs -put  $TMP_BACKUP_DIR/kite_meta.tgz $HDFS_BACKUP_DIR/

