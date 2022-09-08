#!/bin/bash -xue
# Generates the runtime-env.yml configs based on conda-env.yml.

cd $(dirname $0)
cat ../conda-env.yml | grep -Pv -- '^#|- (make|sbt|nodejs|yarn|go|compilers|swig|autopep8|mypy)\b' > runtime-env-cuda.yml
cat runtime-env-cuda.yml | grep -Pv -- '- (rapidsai|nvidia|cugraph|cudatoolkit)\b' | sed 's/cuda\|cu113/cpu/g' > runtime-env.yml
