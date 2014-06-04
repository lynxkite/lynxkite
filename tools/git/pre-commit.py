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
branch = subprocess.check_output('git rev-parse --abbrev-ref=strict HEAD'.split()).strip()
if branch in protected_branches:
  warn('You cannot commit directly to {0!r}.'.format(branch))
  warn('Please create a new branch, commit there, and send a pull request on GitHub.')

diff = subprocess.check_output('git diff --staged'.split())
if 'DO NOT SUBMIT' in diff:
  lines = [l for l in diff.split('\n') if 'DO NOT SUBMIT' in l]
  warn('"DO NOT SUBMIT" found in your diff:')
  for l in lines:
    warn('  ' + l)

if warned:
  sys.exit(1)
