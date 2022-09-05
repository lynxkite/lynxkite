#!/bin/bash -xue
# Downloads and installs Spark. Useful for lazy people and CI systems alike.

cd $(dirname $0)/..
VERSION=$(cat conf/SPARK_VERSION)

# Link to the given name or spark-$VERSION by default.
LINK="${1:-spark-${VERSION}}"

HADOOP='3'
cd $HOME
mkdir -p spark
cd spark
if [[ ! -x "$LINK" ]]; then
  wget -nv \
    "https://archive.apache.org/dist/spark/spark-$VERSION/spark-${VERSION}-bin-hadoop2.7.tgz" \
    -O "spark-${VERSION}-bin-hadoop${HADOOP}.tgz"
  tar xf "spark-${VERSION}-bin-hadoop${HADOOP}.tgz"
  rm "spark-${VERSION}-bin-hadoop${HADOOP}.tgz"
  ln -s "$HOME/spark/spark-${VERSION}-bin-hadoop${HADOOP}" "$LINK"
fi
