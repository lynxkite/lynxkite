set -x
set -e

cd `dirname $0`

sbt test

./e2etest/test_server.py

cd web
grunt jshint
grunt test
