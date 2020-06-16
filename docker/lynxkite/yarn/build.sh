#!/bin/bash -xue
# Builds the LynxKite YARN Docker image.

cd $(dirname $0)

# Move the LynxKite files into the build context, then put them back to the original place.
rm -rf ./stage
mv ../../../target/universal/stage ./stage
mv ./stage/logs ../../../logs.$$ || true
Restore() {
  mv ../../../logs.$$ ./stage/logs || true
  mv ./stage ../../../target/universal/stage
}
trap Restore EXIT

docker build -t lynx/kite_yarn:latest .
