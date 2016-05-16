#!/usr/bin/python

"""
Given a glob pattern, this executes all the Groovy scripts
from its own directory, starting up a separate instance
of LynxKite for each script. (To avoid interactions between
tests, e.g. cache-related.)
The order of script execution is not arbitrary. The groovy
scripts can be annotated with

/// REQUIRE_SCRIPT other_script.groovy

lines, and this script will choose an execution order in which
a groovy script is only executed after all its required scripts were run.
(This only works if the requirement graph has no loops.)

Usage big_data_test_runner.py pattern value:setting
For example xyz/big_data_test_runner.py '*' testDataSet:fake_westeros_100m
Will execute all the tests xyz/*.groovy using the test data from
fake_westeros_100m.
"""

import glob
import os
import sys
import subprocess

def run_test(kite_path, test_path, groovy_args, seen_tests, dry_run):
  """
  Runs a single test script, but before that, runs its
  requirement scripts.
  """
  if test_path in seen_tests:
    return
  seen_tests[test_path] = True

  test_script_dir = os.path.dirname(test_path)
  for line in open(test_path, 'r'):
    if line.startswith('///'):
      REQUIRE_SCRIPT_STR = '/// REQUIRE_SCRIPT '
      if line.startswith(REQUIRE_SCRIPT_STR):
        required_script = line[len(REQUIRE_SCRIPT_STR):].strip()
        required_script_path = test_script_dir + '/' + required_script
        run_test(kite_path, required_script_path, groovy_args, seen_tests, dry_run)
      else:
        print 'Unknown directive in ', test_path, ':', line
        sys.exit(1)
    elif line.startswith('// REQ'):
      print 'Did you mean /// REQUIRE_SCRIPT in ', test_path, ':', line
      sys.exit(1)
  if not dry_run:
    subprocess.call([kite_path, 'batch', test_path] + groovy_args)

def run_tests(kite_path, scripts, groovy_args, dry_run):
  # Set of tests that were seen by the run_test functions.
  # This is used to ensure that one test is executed at most
  # once.
  seen_tests = {}
  for script in scripts:
    run_test(
      kite_path,
      script,
      groovy_args,
      seen_tests,
      dry_run)

def main(argv):
  my_path = os.path.abspath(os.path.dirname(argv[0]))
  if len(argv) < 2:
    print 'Invalid parameters. Usage:'
    print argv[0], ' test_pattern [param1:value1 ...]'
    print 'For example:'
    print argv[0], ' \'*\' testDataSet:fake_westeros_100k'
    return
  else:
    script_pattern = argv[1]
    groovy_args = argv[2:]
  kite_path = my_path + '/../bin/biggraph'
  scripts = glob.glob(my_path + '/' + script_pattern + '.groovy')
  # Ensure the order is deterministic to have nice diffs:
  scripts.sort()
  # Make a dry run first to detect macro errors early:
  run_tests(kite_path, scripts, groovy_args, dry_run=True)
  run_tests(kite_path, scripts, groovy_args, dry_run=False)

if __name__ == "__main__":
  main(sys.argv)
