set -x
set -e

cd `dirname $0`

sbt test
sbt stage

./e2etest/test_server.py

cd web
grunt test
