#!/bin/sh -xue

cd `dirname $0`
./test_backend.sh
./test_frontend.sh
