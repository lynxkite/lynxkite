#!/bin/bash -ue
# Generates smaller dependency lists based on conda-env.yml.
# Usage:
#
#   conda-env.sh [build] [cuda]
#
# The "build" parameter causes build dependencies to be included.
# The "cuda" parameter causes CUDA (GPU) dependencies to be included.
#
# The generated config is printed on stdout.

cd $(dirname $0)
include_cuda='false'
include_build='false'
while [[ $# -gt 0 ]]; do
  case $1 in
    'build')
      include_build='true'
      ;;
    'cuda')
      include_cuda='true'
      ;;
  esac
  shift
done
cfg=$(cat conda-env.yml)
if [[ $include_build == 'false' ]]; then
  cfg=$(echo "$cfg" | grep -Pv -- '^#|- (make|sbt|nodejs|yarn|go|compilers|swig|autopep8|mypy|zip|wget)\b')
fi
if [[ $include_cuda == 'false' ]]; then
  cfg=$(echo "$cfg" | grep -Pv -- '- (rapidsai|nvidia|cugraph|cudatoolkit)\b' | sed 's/cuda\|cu113/cpu/g')
fi
echo "$cfg"
