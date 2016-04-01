#!/bin/bash -xue

cd $(dirname $0)

# Run test. (TODO)
cat kitescripts/perf/last_output.md | sed 's/size/how big/' > kitescripts/perf/last_output.md.new

# Commit and push changed output on PR branch.
git config user.name 'lynx-jenkins'
git config user.email 'pizza-support@lynxanalytics.com'
git checkout "$GIT_BRANCH"
mv kitescripts/perf/last_output.md{.new,}
PERF_CHANGE="12% faster"
git commit -am "Big Data Test: $PERF_CHANGE"
git push
