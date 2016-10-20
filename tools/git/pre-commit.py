#!/usr/bin/env python
from __future__ import print_function
import subprocess
import sys

color = sys.stderr.isatty()
warned = False


def warn(msg):
  global warned
  warned = True
  if color:
    print('\x1b[31m{}\x1b[0m'.format(msg), file=sys.stderr)
  else:
    print(msg, file=sys.stderr)

protected_branches = ['master']
branch = subprocess.check_output(
    'git rev-parse --abbrev-ref=strict HEAD'.split()).strip()
if branch in protected_branches:
  warn('You cannot commit directly to {0!r}.'.format(branch))
  warn('Please create a new branch, commit there, and send a pull request on GitHub.')

# ''.split('\n') -> [''] by default.....
name_status = filter(bool, subprocess.check_output(
    'git diff --name-status --staged'.split()).strip().split('\n'))

files = [line.split()[1] for line in name_status if line.split()[0] != "D"]

diff = subprocess.check_output('git diff --staged'.split())
new_lines = [l for l in diff.split('\n') if l.startswith('+')]

bad_lines = [l for l in new_lines if 'DO NOT SUBMIT' in l]
if bad_lines:
  warn('"DO NOT SUBMIT" found in your diff:')
  for l in bad_lines:
    warn('  ' + l)

non_makefiles = [fn for fn in files if not fn.endswith('Makefile')]
if len(non_makefiles) > 0:
  non_makefile_diff = subprocess.check_output('git diff --staged'.split() + non_makefiles)
  bad_lines = [l for l in non_makefile_diff.split('\n') if l.startswith('+') and '\t' in l]
  if bad_lines:
    warn('TAB found in your diff:')
    for l in bad_lines:
      warn('  ' + l)

if any(fn.endswith('.js') for fn in files):
  if subprocess.call('cd web; gulp jshint', shell=True):
    warn('JSHint fails.')

pythons = [fn for fn in files if fn.endswith('.py')]
if pythons:
  subprocess.call(['autopep8', '-ia'] + pythons)

if warned:
  sys.exit(1)
