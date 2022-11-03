#!/bin/sh -xue
# Builds LynxKite and runs it with "spark-submit" using ~/.kiterc.

cd $(dirname $0)
tools/install_spark.sh
make backend
. ~/.kiterc
spark-submit --conf 'spark.driver.extraJavaOptions=-Dplay.http.secret.key=$KITE_APPLICATION_SECRET' target/scala-2.12/lynxkite-0.1-SNAPSHOT.jar
