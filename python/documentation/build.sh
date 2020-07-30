#!/bin/bash -xue
# Generates the Sphinx documentation and the operations.py file
# containing the Python descriptions of the LynxKite operations.

cd $(dirname "$0")

# Create an empty file at the beginning
cp /dev/null ../remote_api/src/lynx/operations.py 

python3 create_operations_doc.py
make clean html

# Remove "lynx.operations" from the function description in the UI.
sed -i -e 's/lynx\.operations//g' _build/html/operations.html

