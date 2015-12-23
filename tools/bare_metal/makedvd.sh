#!/bin/bash
set -ueo pipefail

if [ $# != 2 ]; then
  >&2 echo "Usage: $0 <LynxKite archive to put on the DVD> <ubuntu OR redhat>"
  exit 1
fi
LYNXKITE_TGZ="$1"
if [ ! -f "$LYNXKITE_TGZ" ]; then
  >&2 echo "ERROR: The specified LynxKite archive ($LYNXKITE_TGZ) does not exist."
  exit 1
fi
set -x

BARE_METAL_DIR=$(dirname "$0")
CONFIG_DIR="$BARE_METAL_DIR/$2"
REPO_DIR="$BARE_METAL_DIR/../.."
BUILD="/tmp/bare-metal-dvd"
SPARK_VERSION=$(cat "$REPO_DIR/conf/SPARK_VERSION")
rm -rf "$BUILD" || true
mkdir "$BUILD"

# Add LynxKite and scripts.
cp "$LYNXKITE_TGZ" "$BUILD"
cp "$BARE_METAL_DIR"/*.sh "$BUILD"
cp "$CONFIG_DIR"/*.sh "$BUILD"

# Get Spark.
SPARK_TGZ="spark-$SPARK_VERSION-bin-hadoop2.4.tgz"
wget --directory-prefix="$BUILD/" \
  "http://d3kbcqa49mib13.cloudfront.net/$SPARK_TGZ"

# Get Cloudera.
. $CONFIG_DIR/config.sh

wget --directory-prefix="$BUILD/" $CLOUDERA_MANAGER_URL
wget --directory-prefix="$BUILD/" $CLOUDERA_CDH_PARCEL_URL
wget --directory-prefix="$BUILD/" $CLOUDERA_CDH_PARCEL_SHA1_URL
wget --directory-prefix="$BUILD/" $CLOUDERA_MANIFEST_URL

# Get Oracle Server JRE 7.
wget --directory-prefix="$BUILD/" \
  --header='Cookie: oraclelicense=accept-securebackup-cookie' \
  'http://download.oracle.com/otn-pub/java/jdk/7u80-b15/server-jre-7u80-linux-x64.tar.gz'

# Create DVD image.
mkisofs -r -o "bare-metal-dvd.iso" "$BUILD"
echo "DVD image successfully created as: bare-metal-dvd.iso"
