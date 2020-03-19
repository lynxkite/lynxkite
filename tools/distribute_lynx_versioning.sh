#!/bin/bash -xue
#
# Create a symbolic link to lynx_versioning.py to beside all setup.py files
# in our repository.

ROOT=`git rev-parse --show-toplevel`
cd $ROOT

SRC=python/versioning/lynx_versioning.py
git ls-files | grep "/setup[.]py" | while read f; do
    DST=`dirname $f`
    pushd $DST > /dev/null
    if [[ ! -L lynx_versioning.py ]]; then
        RELATIVE=`python3 -c "d='$DST'; n = len(d.split('/')); print ('../'*n + '$SRC')"`
        ln -s  $RELATIVE lynx_versioning.py
    fi
    popd > /dev/null
done

