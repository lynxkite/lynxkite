#!/bin/bash -ue

RANDOM_SUFFIX=$(python -c \
  'import random, string; print "".join(random.choice(string.letters) for i in range(6))')
TODAY=$(date "+%Y%m%d")
RANDOM_NAME="$1${TODAY}_${RANDOM_SUFFIX}"

# Prepare the overrides file.
OVERRIDES_FILE="/tmp/$(basename $RANDOM_NAME).overrides"

cat > "$OVERRIDES_FILE" <<EOF
export KITE_META_DIR="\${KITE_META_DIR}/${RANDOM_NAME}"
export KITE_DATA_DIR="\${KITE_DATA_DIR}/${RANDOM_NAME}"
EOF

echo "$OVERRIDES_FILE"
