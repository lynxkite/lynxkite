#!/bin/bash -xue
# Downloads and installs Spark. Useful for lazy people and Jenkins alike.

cd $(dirname $0)/..
VERSION=$(cat conf/SPARK_VERSION)
HADOOP='2.6'
cd $HOME
if [[ ! -x "spark-${VERSION}" ]]; then
  wget "http://d3kbcqa49mib13.cloudfront.net/spark-${VERSION}-bin-hadoop${HADOOP}.tgz"
  tar xf "spark-${VERSION}-bin-hadoop${HADOOP}.tgz"
  rm "spark-${VERSION}-bin-hadoop${HADOOP}.tgz"
  ln -s "spark-${VERSION}-bin-hadoop${HADOOP}" "spark-${VERSION}"
fi
