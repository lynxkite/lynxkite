#!/bin/bash -eu
# Script for generating PDF versions of the documentation pages.

WKHTMLTOPDF="${WKHTMLTOPDF:-wkhtmltopdf}"
WKHTML_OPT="--lowquality --footer-center [page] --margin-top 20mm --margin-bottom 20mm"

echo "Starting Grunt server..."
"$(dirname $0)/../grunt.sh" &
GRUNT_PID=$!
function kill_grunt {
  pkill -term -P $GRUNT_PID
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

