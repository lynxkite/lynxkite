#!/bin/bash -xue
# Starts up a LynxKite instance for testing, runs a command, then shuts down LynxKite.

TOOLS=$(dirname $0)
cd $TOOLS
pip install -e ../python/remote_api
cat << EOF > with_lk.py
# Generated from with_lk.sh.
from pyspark.sql import SparkSession
from lynx.kite import LynxKite
import os, sys, subprocess

# Set up Spark and LynxKite.
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('OFF')
lk = LynxKite(spark=spark)
print('LynxKite is running at', lk._address)
os.environ['LYNXKITE_ADDRESS'] = lk._address

# Run the specified command.
subprocess.run(sys.argv[1:], check=True)
EOF
# SQLite-JDBC is used for testing JDBC.
SQLITE_VERSION=3.40.0.0
if [ ! -f sqlite-jdbc-$SQLITE_VERSION.jar ]; then
  wget https://github.com/xerial/sqlite-jdbc/releases/download/$SQLITE_VERSION/sqlite-jdbc-$SQLITE_VERSION.jar
fi
cd -
export KITE_ALLOW_NON_PREFIXED_PATHS=true
spark-submit \
  --driver-class-path $TOOLS/sqlite-jdbc-$SQLITE_VERSION.jar \
  --jars $TOOLS/../target/scala-2.12/lynxkite-0.1-SNAPSHOT.jar \
  $TOOLS/with_lk.py "$@"
