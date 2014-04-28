#!/bin/sh -xue
# Sets up local hooks. Run this after cloning the repository.
HERE=$(dirname $0)
ROOT=$HERE/../..
ln -s "../../$HERE/pre-commit.py" "$ROOT/.git/hooks/pre-commit"
