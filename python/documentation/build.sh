#!/bin/bash -xue
# Generates the Sphinx documentation and the operations.py file
# containing the Python descriptions of the LynxKite operations.

cd $(dirname "$0")

python3 create_operations_doc.py
make clean html

# Remove "lynx.operations" from the function description in the UI.
sed -i -e 's/lynx\.operations//g' _build/html/operations.html

