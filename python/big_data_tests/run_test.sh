#!/bin/bash -xue
# Run this script to update the results.txt file.

cd $(dirname $0)

mkdir -p meta
mkdir -p data

rm -rf meta/*
for i in uploads tables scalars.json partitioned operations models exports broadcasts; do
    rm -rf data/$i
done

function download() {
    cd data
    if [[ ! -d "$1" ]]; then
        mkdir $1
        aws s3 cp --recursive s3://lynxkite-test-data/$1 $1
    fi      
    cd ..
}

download local_test_vertices.parquet
download local_test_edges.parquet

HERE=`pwd`

cat <<EOF > kiterc
export SPARK_HOME=$HOME/spark-\${SPARK_VERSION}
export SPARK_MASTER=local
export KITE_META_DIR=${HERE}/meta
export KITE_DATA_DIR=file:${HERE}/data
export KITE_PID_FILE=${HERE}/kite.pid
export KITE_USERS_FILE=${HERE}/kite_users.txt
export NUM_CORES_PER_EXECUTOR=2
export KITE_MASTER_MEMORY_MB=20000
export KITE_HTTP_PORT=33087
EOF

./test_driver.sh &

export KITE_SITE_CONFIG=${HERE}/kiterc
../../stage/bin/biggraph interactive


