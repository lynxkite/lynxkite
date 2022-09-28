#!/bin/sh -xue
# Builds LynxKite and runs it with "spark-submit" using ~/.kiterc.

cd $(dirname $0)
tools/install_spark.sh
make backend
. ~/.kiterc
$SPARK_HOME/bin/spark-submit \
  target/scala-2.12/lynxkite-0.1-SNAPSHOT.jar
