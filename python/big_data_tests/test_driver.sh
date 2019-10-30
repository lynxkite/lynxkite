#!/bin/bash
# Called by run_tests.sh (in the background). This script
# runs the big data tests on LynxKite and then kills LynxKite.

. kiterc

../../tools/wait_for_port.sh $KITE_HTTP_PORT
export LYNXKITE_ADDRESS=http://localhost:$KITE_HTTP_PORT
./big_data_tests.py --vertex_file 'DATA$/local_test_vertices.parquet' --edge_file 'DATA$/local_test_edges.parquet' > results.txt

sleep 5

kill `cat $KITE_PID_FILE`
