#!/bin/bash -eu
# Script for generating PDF versions of the documentation pages.

CHROME_MAIN_VERSION=$(google-chrome --version | grep -oE " [0-9]+")
NODE_MAIN_VERSION=$(node --version | grep -oE "v[0-9]+" | tr -d 'v')
NODE_SUB_VERSION=$(node --version | grep -oE "\.[0-9]+\." | tr -d '.')
if (( CHROME_MAIN_VERSION < 59 )); then
  >&2 echo 'Please install Google Chrome 59 or newer or set google-chrome to point to the binary.'
  exit 1
fi
if (( $NODE_MAIN_VERSION < 6 || $NODE_MAIN_VERSION == 6 && $NODE_SUB_VERSION < 4)); then
  >&2 echo 'Please install Node 6.4.0 or newer or set node to point to the binary.'
  exit 1
fi

echo 'Starting LynxKite...'
cd "$(dirname $0)/../web"
npm run dev &
LYNXKITE_PID=$!
cd ..
npm install puppeteer
function kill_dev_server {
  echo 'Shutting down LynxKite...'
  kill $LYNXKITE_PID
}
trap kill_dev_server EXIT
# Wait until the dev server is up.
tools/wait_for_port.sh 5173
echo # Add new-line after dev server output.

echo 'Generating User Manual...'
node tools/chrome_print_pdf.js 'http://localhost:9090/pdf-help' 'LynxKite-User-Manual.pdf'

echo 'Generating Admin Manual...'
node tools/chrome_print_pdf.js 'http://localhost:9090/pdf-admin-manual' 'LynxKite-Administrator-Manual.pdf'

echo 'LynxKite documentation generated successfully.'
