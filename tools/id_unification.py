#!/usr/bin/env python3
'''Cleans up our mixed use of "ID" and "Id". We decided to go with "Id"!'''

import re
import subprocess


def cleanup(code):
  r = re.compile(r'(?<=[a-z])ID')
  lines = []
  for line in code.split('\n'):
    if 'new hadoop' not in line and 'image/png' not in line:
      line = r.sub('Id', line)
    lines.append(line)
  return '\n'.join(lines)


def cleanup_file(fn):
  if (re.match(r'.*\.(json|png|cert|jar|jpg|sqlite)$', fn) or
          re.match(r'public|resources/shapefiles|stage|test/resources', fn)):
    return
  with open(fn) as f:
    content = f.read()
  clean = cleanup(content)
  if clean != content:
    print(fn)
    with open(fn, 'w') as f:
      f.write(clean)

p = subprocess.run('git ls-files'.split(), stdout=subprocess.PIPE)
output = p.stdout.decode('utf-8').strip()
for fn in output.split('\n'):
  cleanup_file(fn)
