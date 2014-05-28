set -x
set -e

cd `dirname $0`

sbt test

./e2etest/test_server.py

cd web
bower install --silent --offline --config.interactive=false
npm install --offline
grunt jshint
grunt test
