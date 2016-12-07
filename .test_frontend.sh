#!/bin/sh -xue

cd `dirname $0`

tools/with_lk.sh tools/e2e_test.sh
