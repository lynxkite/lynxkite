#!/bin/sh -xue
# Builds LynxKite and runs it with "spark-submit" using ~/.kiterc.

cd $(dirname $0)
make backend
. ~/.kiterc
spark-submit target/scala-2.12/lynxkite-0.1-SNAPSHOT.jar
