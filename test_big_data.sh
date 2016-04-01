#!/bin/bash -xue

env
cd $(dirname $0)
cat kitescripts/perf/last_output.md | sed 's/size/how big/' > kitescripts/perf/last_output.md.new
mv kitescripts/perf/last_output.md{.new,}
git status
PERF_CHANGE="12% faster"
git commit --author="lynx-jenkins <pizza-support@lynxanalytics.com>" -am "Big Data Test: $PERF_CHANGE"
exit 1
git push
