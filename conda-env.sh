#!/bin/bash -ue
# Generates smaller dependency lists based on conda-env.yml.
# Usage:
#
#   conda-env.sh [build]
#
# The "build" parameter causes build dependencies to be included.
#
# The generated config is printed on stdout.

cd $(dirname $0)
include_build='false'
while [[ $# -gt 0 ]]; do
  case $1 in
    'build')
      include_build='true'
      ;;
  esac
  shift
done
cfg=$(cat conda-env.yml)
if [[ $include_build == 'false' ]]; then
  cfg=$(echo "$cfg" | grep -Pv -- '^#|- (make|sbt|nodejs|go|compilers|swig|zip|wget)\b')
fi
echo "$cfg"
