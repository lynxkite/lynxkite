#!/bin/bash -eu
# Script for generating PDF versions of the documentation pages.

WKHTMLTOPDF="${WKHTMLTOPDF:-wkhtmltopdf}"
if [ ! -x "$WKHTMLTOPDF" ]; then
  >&2 echo 'Please install wkhtmltopdf or set WKHTMLTOPDF to point to the binary.'
  exit 1
fi
WKHTML_OPT="--lowquality --footer-center [page] --margin-top 20mm --margin-bottom 20mm"

echo "Starting Grunt server..."
cd "$(dirname $0)/../web"
grunt serve:dist &
GRUNT_PID=$!
cd -
function kill_grunt {
  kill $GRUNT_PID
}
trap kill_grunt EXIT
# Wait until Grunt is up.
while ! nc -z localhost 9090; do sleep 1; done
echo

echo "Generating User Guide..."
"$WKHTMLTOPDF" $WKHTML_OPT \
  'http://localhost:9090/pdf-help' 'LynxKite-User-Manual.pdf'

echo "Generating Admin Manual..."
"$WKHTMLTOPDF" $WKHTML_OPT \
  'http://localhost:9090/pdf-admin-manual' 'LynxKite-Administrator-Manual.pdf'

echo "LynxKite documentation generated successfully."

