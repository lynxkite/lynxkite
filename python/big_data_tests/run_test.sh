#!/bin/bash -xue
# Run this script to update the results.txt file.

cd $(dirname $0)

mkdir -p meta
mkdir -p data
mkdir -p parquet

rm -rf meta/*
rm -rf data/*

function download() {
    cd parquet
    if [[ ! -d "$1" ]]; then
        mkdir $1
        aws s3 cp --recursive s3://lynxkite-test-data/$1 $1
    fi
    cd ..
}

download local_test_vertices.parquet
download local_test_edges.parquet

HERE=`pwd`
KITEPORT=33087

cat <<EOF > kiterc
export SPARK_HOME=$HOME/spark/spark-\${SPARK_VERSION}
export SPARK_MASTER=local
export KITE_META_DIR=${HERE}/meta
export KITE_DATA_DIR=file:${HERE}/data
export KITE_PID_FILE=${HERE}/kite.pid
export KITE_PREFIX_DEFINITIONS=${HERE}/prefix_definitions.txt
export KITE_USERS_FILE=${HERE}/kite_users.txt
export NUM_CORES_PER_EXECUTOR=2
export KITE_MASTER_MEMORY_MB=20000
export KITE_HTTP_PORT=$KITEPORT
EOF

cat <<EOF > prefix_definitions.txt
PARQUET="file:${HERE}/parquet/"
EOF

export KITE_SITE_CONFIG=${HERE}/kiterc
../../stage/bin/lynxkite start

function stop_kite {
  ../../stage/bin/lynxkite stop
}
trap stop_kite EXIT

../../tools/wait_for_port.sh $KITEPORT
export LYNXKITE_ADDRESS=http://localhost:$KITEPORT
./big_data_tests.py --vertex_file 'PARQUET$/local_test_vertices.parquet' \
                    --edge_file 'PARQUET$/local_test_edges.parquet' > results.txt
