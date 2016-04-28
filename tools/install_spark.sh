#!/bin/bash -xue
# Downloads and installs Spark. Useful for lazy people and Jenkins alike.

cd $(dirname $0)/..
VERSION=$(cat conf/SPARK_VERSION)
cd $HOME
if [[ ! -x "spark-${VERSION}" ]]; then
  wget "http://d3kbcqa49mib13.cloudfront.net/spark-${VERSION}-bin-hadoop2.4.tgz"
  tar xf "spark-${VERSION}-bin-hadoop2.4.tgz"
  rm "spark-${VERSION}-bin-hadoop2.4.tgz"
  ln -s "spark-${VERSION}-bin-hadoop2.4" "spark-${VERSION}"
fi
