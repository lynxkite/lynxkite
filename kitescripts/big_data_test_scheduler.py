#!/usr/bin/python

"""
Generates a list of shell commands to be used for running
big data tests on LynxKite. Prints it to the standard output.
The printed list will look like this:
path/to/biggraph batch script1.groovy | tee ...
path/to/biggraph batch script2.groovy | tee ...
path/to/biggraph batch script3.groovy | tee ...
The order of scripts will not be arbitrary. The groovy scripts
can be annotated with

/// REQUIRE_SCRIPT other_script.groovy

lines, and this script will choose an execution order in which
a groovy script is only executed after all its required scripts were
completed. (This only works if the requirement graph has no loops.)
"""

import optparse
import glob
import os
import sys


def parse_args(argv):
  parser = optparse.OptionParser()
  parser.add_option(
      '--remote_lynxkite_path',
      help='Path to the LynxKite executable on the machine where '
           'the test will be executed. Will be invoked by the '
           'generated script.')
  parser.add_option(
      '--test_selector',
      help='A .list file containing the list of .groovy test files,'
           'or a .groovy file to be executed as a test')
  parser.add_option(
      '--local_test_dir',
      help='Directory from where to parse the test codes for '
           'dependencies by this program.')
  parser.add_option(
      '--remote_test_dir',
      help='Directory from where to run the tests in the '
           'generated script.')
  parser.add_option(
      '--remote_output_dir',
      help='Location where results will be written by the '
           'generated script.')
  parser.add_option('--lynxkite_arg', action='append')
  (options, args) = parser.parse_args()
  return options


def run_lynx_kite(test_name, options):
  print(
      '{remote_lynxkite_path} batch {remote_test_dir}/{test_name} {test_args}'
      ' 2>&1 | tee {remote_output_dir}/{test_basename}.out.txt').format(
      test_name=test_name,
      test_args=' '.join(options.lynxkite_arg or []),
      test_basename=test_name.split('.')[0],
      **vars(options))


def run_test(test_name, options, tests_seen):
  """
  Runs a single test script, but before that, runs its
  requirement scripts.
  """

  if test_name in tests_seen:
    return
  tests_seen[test_name] = True

  local_test_path = options.local_test_dir + '/' + test_name
  for line in open(local_test_path, 'r'):
    if line.startswith('///'):
      REQUIRE_SCRIPT_STR = '/// REQUIRE_SCRIPT '
      if line.startswith(REQUIRE_SCRIPT_STR):
        required_script = line[len(REQUIRE_SCRIPT_STR):].strip()
        run_test(required_script, options, tests_seen)
      else:
        print 'Unknown directive in ', test_path, ':', line
        sys.exit(1)
    elif line.startswith('// REQ'):
      print 'Did you mean /// REQUIRE_SCRIPT in ', test_path, ':', line
      sys.exit(1)
  run_lynx_kite(test_name, options)


def run_tests(scripts, options):
  # Map of tests to results. This is also used for ensuring
  # that one test is executed at most once.
  tests_seen = {}
  for line in scripts:
    line = line.strip()
    if len(line) > 0 and line[0] != '#':
      run_test(line, options, tests_seen)


def get_script_list(options):
  scripts = []
  excluded_scripts = []
  test_selector = options.test_selector
  if (test_selector.endswith('.list')):
    for line in open(options.local_test_dir + '/' + test_selector, 'r'):
      sline = line.strip()
      if sline == '':
        pass
      elif sline == '*':
        matches = glob.glob(options.local_test_dir + '/*.groovy')
        scripts += map(lambda path: path.split('/')[-1], matches)
      elif sline.startswith('-'):
        excluded_scripts.append(sline[1::])
      else:
        scripts.append(sline)
  elif (test_selector.endswith('.groovy')):
    scripts.append(test_selector)
  return filter(lambda script: script not in excluded_scripts, scripts)


def main(argv):
  options = parse_args(argv)
  assert options.test_selector, '--test_selector is required'
  assert options.local_test_dir, '--local_test_dir is required'
  assert options.remote_output_dir, '--remote_output_dir is required'
  assert options.remote_test_dir, '--remote_test_dir is required'

  scripts = get_script_list(options)
  if scripts == []:
    sys.stderr.write('No tests specified.\n')
    sys.exit(1)
  run_tests(scripts, options)

if __name__ == "__main__":
  main(sys.argv)
