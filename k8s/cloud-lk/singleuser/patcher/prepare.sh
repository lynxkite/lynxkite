#!/bin/bash -xue
# Applies the patch to the minified JavaScript of JupyterLab.

cd $(dirname $0)
VENDORSJS=/opt/conda/share/jupyter/lab/static/vendors~main.eb765bbfa0b3d66a41e4.js
# tsx will throw errors about undefined variables. ("React" and "e".) That's fine.
npx tsc --jsx react patch.tsx || true
# In the minified code React is "v".
sed -i 's/React/v/g' patch.js
# Drop semicolon from the last line. This needs to be an expression, not a statement.
sed -i '$ s/;//' patch.js
# Put it all on one line.
PATCH=$(cat patch.js | tr '\n' ' ')
# Inject it right before div.jp-Launcher-cwd.
sed -i "s^v\\[\"createElement\"\\](\"div\",{className:\"jp-Launcher-cwd\"}^$PATCH,\\0^" $VENDORSJS
