#!/bin/sh -xue

cd `dirname $0`

tools/e2e_test.sh
tools/check_documentation.sh
tools/gen_documentation.sh
