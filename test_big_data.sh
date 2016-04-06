#!/bin/bash -xue

cd $(dirname $0)

# Run test.
NUM_INSTANCES=3 \
  tools/emr_based_test.sh perf kitescripts/big_data_tests/edge_import.groovy -- testSet:fake_westeros_100m \
  | tee > kitescripts/big_data_tests/full_output
# Take the header.
cat kitescripts/big_data_tests/last_output.md \
  | sed -n -e '1,/```/p' \
  > kitescripts/big_data_tests/last_output.md.new
# Add the script output from the new run.
cat kitescripts/big_data_tests/full_output \
  | awk '/STARTING SCRIPT/{flag=1}/FINISHED SCRIPT/{print;flag=0}flag' \
  >> kitescripts/big_data_tests/last_output.md.new
echo '```' >>kitescripts/big_data_tests/last_output.md.new
rm kitescripts/big_data_tests/full_output

if [[ "$USER" == 'jenkins' ]]; then
  # Commit and push changed output on PR branch.
  git config user.name 'lynx-jenkins'
  git config user.email 'pizza-support@lynxanalytics.com'
  git config push.default simple
  export GIT_SSH_COMMAND='ssh -i ~/.ssh/lynx-jenkins'
  git fetch
  git checkout "$GIT_BRANCH"
  git reset --hard "origin/$GIT_BRANCH"  # Discard potential local changes from failed runs.
  mv kitescripts/big_data_tests/last_output.md{.new,}
  git commit -am "Update Big Data Test results."
  git push
fi
