#!/bin/sh -xue
# Sets up local hooks. Run this after cloning the repository.
cd $(dirname $0)/../../.git/hooks
ln -s ../../tools/git/pre-commit.py pre-commit
