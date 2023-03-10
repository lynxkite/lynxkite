#!/usr/bin/env python
import contextlib
import subprocess
import sys
import hashlib

color = sys.stderr.isatty()
warned = False


def warn(msg):
  global warned
  warned = True
  if color:
    print('\x1b[31m{}\x1b[0m'.format(msg), file=sys.stderr)
  else:
    print(msg, file=sys.stderr)


def get_hashes(files):
  hashes = []
  for f in files:
    with open(f, 'rb') as file:
      hashes.append(hashlib.md5(file.read()).hexdigest())
  return hashes


def check_output(command):
  return subprocess.check_output(command).decode('utf-8').strip()


protected_branches = ['master', 'boxes']
branch = check_output('git rev-parse --abbrev-ref=strict HEAD'.split())
if branch in protected_branches:
  warn('You cannot commit directly to {0!r}.'.format(branch))
  warn('Please create a new branch, commit there, and send a pull request on GitHub.')

name_status = check_output('git diff --name-status --staged'.split())
name_status = name_status.split('\n') if name_status else []
files = [line.split('\t')[-1] for line in name_status if line.split('\t')[0] != "D"]

diff = check_output('git diff --staged'.split())
new_lines = [l for l in diff.split('\n') if l.startswith('+')]

bad_lines = [l for l in new_lines if 'DO NOT SUBMIT' in l]
if bad_lines:
  warn('"DO NOT SUBMIT" found in your diff:')
  for l in bad_lines:
    warn('  ' + l)


@contextlib.contextmanager
def watch_files(files, context):
  before = get_hashes(files)
  yield
  after = get_hashes(files)
  different = [f[0] for f in zip(files, before, after) if f[1] != f[2]]
  if len(different) > 0:
    warn(f'Files altered by {context}, please restage.')
    warn('Altered files:')
    warn(', '.join(different))


for proj in ['web', 'shell_ui']:
  prefix = proj + '/'
  javascripts = [fn for fn in files if fn.startswith(prefix) and fn.endswith('.js')]
  if javascripts:
    with watch_files(javascripts, 'eslint'):
      localpaths = [fn[len(prefix):] for fn in javascripts]
      cmd = ['npm', 'run', 'eslint', '--', '--fix'] + localpaths
      if subprocess.call(cmd, cwd=proj):
        warn('ESLint failed.')


def linter(extension, command):
  matched = [fn for fn in files if fn.endswith(extension)]
  if matched:
    with watch_files(javascripts, ' '.join(command)):
      subprocess.call(command + matched)


def check_templates():
  if [f for f in files if
      f.endswith('kiterc.asciidoc') or
      f.endswith('prefix_definitions.asciidoc') or
      f.endswith('kiterc_template') or
          f.endswith('prefix_definitions_template.txt')]:
    with watch_files(['conf/kiterc_template', 'conf/prefix_definitions_template.txt'], 'gen_templates.py'):
      subprocess.call(['tools/gen_templates.py'])


linter('.py', ['autopep8', '-ia'])
linter('.go', ['go', 'fmt'])
check_templates()

if warned:
  sys.exit(1)
