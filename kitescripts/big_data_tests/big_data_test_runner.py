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
For example xyz/big_data_test_runner.py '*' testSet:fake_westeros_100m
Will execute all the tests xyz/*.groovy using the test data from
fake_westeros_100m.
"""

# TODO: support multiple patterns and multiple arguments

import glob
import os
import sys
import subprocess

# Set of tests that were seen by the run_test functions.
# This is used to ensure that one test is executed at most
# once.
seen_tests = {}

def run_test(kite_path, test_dir, test_name, test_data_set):
  """
  Runs a single test script, but before that, runs its
  requirement scripts.
  """
  if test_name in seen_tests:
    return
  seen_tests[test_name] = True

  file_name = test_dir + '/' + test_name
  for line in open(file_name, 'r'):
    if line.startswith('///'):
      REQUIRE_SCRIPT_STR = '/// REQUIRE_SCRIPT '
      if line.startswith(REQUIRE_SCRIPT_STR):
        required_script = line[len(REQUIRE_SCRIPT_STR):].strip()
        run_test(kite_path, test_dir, required_script, test_data_set) 
      else:
        print 'Unknown directive in ', file_name, ': ', line
        sys.exit(1)

  subprocess.call([kite_path, 'batch', file_name, test_data_set])

def main(argv):
  my_path = os.path.abspath(os.path.dirname(argv[0]))
  if len(argv) < 3:
    print 'Invalid parameters. Usage:'
    print argv[0], ' pattern parameter'
    print 'For example:'
    print argv[0], ' \'*\' testSet:fake_westeros_100k'
    return
  else:
    script_pattern = argv[1]
    test_data_set = argv[2]
  kite_path = my_path + '/../../bin/biggraph'
  scripts = glob.glob(my_path + '/' + script_pattern + '.groovy')
  # Ensure the order is deterministic to have nice diffs:
  scripts.sort()
  for script in scripts:
    run_test(
      kite_path,
      my_path,
      os.path.relpath(script, my_path),
      test_data_set)

if __name__ == "__main__":
  main(sys.argv)
