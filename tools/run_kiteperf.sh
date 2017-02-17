#!/bin/bash
#
#  Runs the default big data test for a very fresh state of the master branch.
#  Our real goal is the side effect of test_big_data.py: that will upload the
#  kite performance logs to gs, and redash will be able to collect the data
#  from there.

source "$(dirname $0)/tools/biggraph_common.sh"

LOG=`git log  master --merges --first-parent master --oneline  --format='%H %at %ai %cn %s' -1`
COMMIT=`echo $LOG | awk '{print $1}'`
TIMESTAMP=`echo $LOG | awk '{print $2}'`
echo "log: $LOG"


# We are cloning to start from scratch: this way running the script will not interfere
# with whatever else you're doing.

TMPDIR=`mktemp -d`
echo "Cloning to $TMPDIR"
BIGGRAPH_REPO='git@github.com:biggraph/biggraph.git'
git clone "$BIGGRAPH_REPO" "$TMPDIR"
pushd $TMPDIR > /dev/null
git checkout $COMMIT
make ecosystem

KITE_INSTANCE_NAME="kiteperf_${TIMESTAMP}_${COMMIT}"
./test_big_data.py --emr_instance_count 4 \
                   --rm \
                   --task DefaultTests \
                   --test_set_size medium \
                   --lynx_release_dir ecosystem/native/dist \
                   --kite_instance_name ${KITE_INSTANCE_NAME}

popd > /dev/null
rm -rf $TMPDIR


