#!/bin/bash +xue
#
# Copy the file lynx_versioning.py to beside all setup.py files
# in our repository.

cd `git rev-parse --show-toplevel`

SRC=python/versioning/lynx_versioning.py
git ls-files | grep "/setup[.]py" | while read f; do
    DST=`dirname $f`/lynx_versioning.py
    echo "# GENERATED FILE, DO NOT EDIT!!!" > $DST
    cat $SRC >> $DST
done

