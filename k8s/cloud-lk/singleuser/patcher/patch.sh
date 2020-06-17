#!/bin/bash -xue
# Applies the patch to the minified JavaScript of JupyterLab.

cd $(dirname $0)
VENDORSJS=/opt/conda/share/jupyter/lab/static/vendors~main.eb765bbfa0b3d66a41e4.js
sed -i "s^LYNXKITE_ADDRESS^$LYNXKITE_ADDRESS^g" $VENDORSJS
sed -i "s^LYNXKITE_SIGNED_TOKEN_URL^$LYNXKITE_SIGNED_TOKEN_URL^g" $VENDORSJS
