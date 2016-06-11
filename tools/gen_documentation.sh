#!/bin/bash -eu
# Script for generating PDF versions of the documentation pages.

# Using which with a safe default so that this works in -u mode.
WKHTMLTOPDF="${WKHTMLTOPDF:-$(which wkhtmltopdf || echo '')}"

if [ ! -x "$WKHTMLTOPDF" ]; then
  >&2 echo 'Please install wkhtmltopdf (0.12.3 or newer) or set WKHTMLTOPDF to point to the binary.'
  exit 1
fi
WKHTML_OPT='--lowquality --footer-center [page] --margin-top 20mm --margin-bottom 20mm --print-media-type'

echo 'Starting LynxKite...'
cd "$(dirname $0)/../web"
gulp serve &
LYNXKITE_PID=$!
cd -
function kill_grunt {
  echo 'Shutting down LynxKite...'
  kill $LYNXKITE_PID
}
trap kill_grunt EXIT
# Wait until Grunt is up.
tools/wait_for_port.sh 9090
echo # Add new-line after Grunt output.

echo 'Generating User Manual...'
"$WKHTMLTOPDF" $WKHTML_OPT \
  'http://localhost:9090/pdf-help' 'LynxKite-User-Manual.pdf'

echo 'Generating Admin Manual...'
"$WKHTMLTOPDF" $WKHTML_OPT \
  'http://localhost:9090/pdf-admin-manual' 'LynxKite-Administrator-Manual.pdf'

echo 'Generating Academy...'
"$WKHTMLTOPDF" $WKHTML_OPT \
  'http://localhost:9090/pdf-academy' 'LynxKite-Academy.pdf'

echo 'LynxKite documentation generated successfully.'
