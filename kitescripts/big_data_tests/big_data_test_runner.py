#!/usr/bin/python

# Given a glob pattern, this executes all the Groovy scripts
# from its own directory, starting up a separate instance
# of LynxKite for each script. (To avoid interactions between
# tests, e.g. cache-related.)
# The order of script execution is not arbitrary. The groovy
# scripts can be annotated with
#
# /// REQUIRE_SCRIPT other_script.groovy
#
# lines, and if this script will choose an execution order in which
# a groovy script is only executed after all its required scripts were run.
# (This only works if the requirement graph has no loops.)

# Usage big_data_test_runner.py pattern value:setting
# For example xyz/big_data_test_runner.py '*' testSet:fake_westeros_100m
# Will execute all the tests xyz/*.groovy using the test data from
# fake_westeros_100m.

# TODO: support multiple patterns and multiple arguments

import glob
import os
import sys
import subprocess

# Set of tests that were seen by the runTest functions.
# This is used to ensure that one test is executed at most
# once.
seenTests = {}

def runTest(kitePath, testDir, testName, testDataSet):
  if testName in seenTests:
    return
  seenTests[testName] = True

  fileName = testDir + '/' + testName
  for line in open(fileName, 'r'):
    if line.startswith('///'):
      REQUIRE_SCRIPT_STR = '/// REQUIRE_SCRIPT '
      if line.startswith(REQUIRE_SCRIPT_STR):
        requiredScript = line[len(REQUIRE_SCRIPT_STR):].strip()
        runTest(kitePath, testDir, requiredScript, testDataSet) 
      else:
        print 'Unknown directive in ', fileName, ': ', line
        sys.exit(1)

  subprocess.call([kitePath, 'batch', fileName])

def main(argv):
  myPath = os.path.abspath('/'.join(argv[0].split('/')[:-1]))
  if len(argv) < 3:
    print 'Invalid parameters. Usage:'
    print argv[0], ' pattern parameter'
    print 'For example:'
    print argv[0], ' \'*\' testSet:fake_westeros_100k'
    return
  else:
    scriptFilter = argv[1]
    testSetFilter = argv[2]
  kitePath = myPath + '/../../bin/biggraph'
  scripts = glob.glob(myPath + '/' + scriptFilter + '.groovy')
  # Ensure the order is deterministic to have nice diffs:
  scripts.sort()
  for script in scripts:
    runTest(kitePath, myPath, script.split('/')[-1], testSetFilter)

if __name__ == "__main__":
  main(sys.argv)
