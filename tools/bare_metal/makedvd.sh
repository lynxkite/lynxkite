#!/bin/bash
set -ueo pipefail

if [ $# != 1 ]; then
  >&2 echo "Usage: $0 <LynxKite archive to put on the DVD.>"
  exit 1
fi
LYNXKITE_TGZ="$1"
if [ ! -f "$LYNXKITE_TGZ" ]; then
  >&2 echo "ERROR: The specified LynxKite archive ($LYNXKITE_TGZ) does not exist."
  exit 1
fi
set -x

BARE_METAL_DIR=$(dirname "$0")
REPO_DIR="$BARE_METAL_DIR/../.."
BUILD="/tmp/bare-metal-dvd"
SPARK_VERSION=$(cat "$REPO_DIR/conf/SPARK_VERSION")
rm -rf "$BUILD" || true
mkdir "$BUILD"

# Add LynxKite and scripts.
cp "$LYNXKITE_TGZ" "$BUILD"
cp "$BARE_METAL_DIR"/*.sh "$BUILD"

# Get Spark.
SPARK_TGZ="spark-$SPARK_VERSION-bin-hadoop2.4.tgz"
wget --directory-prefix="$BUILD/" \
  "http://d3kbcqa49mib13.cloudfront.net/$SPARK_TGZ"

# Get Cloudera.
wget --directory-prefix="$BUILD/" \
  'http://archive-primary.cloudera.com/cm5/cm/5/cloudera-manager-trusty-cm5.3.3_amd64.tar.gz'
wget --directory-prefix="$BUILD/" \
  'http://archive.cloudera.com/cdh5/parcels/5.3.3/CDH-5.3.3-1.cdh5.3.3.p0.5-trusty.parcel'
wget --directory-prefix="$BUILD/" \
  'http://archive.cloudera.com/cdh5/parcels/5.3.3/CDH-5.3.3-1.cdh5.3.3.p0.5-trusty.parcel.sha1'
wget --directory-prefix="$BUILD/" \
  'http://archive.cloudera.com/cdh5/parcels/5.3.3/manifest.json'

# Get Oracle Server JRE 7.
wget --directory-prefix="$BUILD/" \
  'http://download.oracle.com/otn-pub/java/jdk/7u80-b15/server-jre-7u80-linux-x64.tar.gz'

# Create DVD image.
mkisofs -r -o "bare-metal-dvd.iso" "$BUILD"
echo "DVD image successfully created as: bare-metal-dvd.iso"
