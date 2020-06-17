#!/usr/bin/env python3
'''Formats the YAML files with ruamel.yaml.'''
import os
from ruamel import yaml


def str_representer(dumper, s):
  if '\n' in s:
    style = '|'
  elif len(s) > 60:
    style = '>'
  else:
    style = None
  return dumper.represent_scalar('tag:yaml.org,2002:str', s, style=style)


yaml.representer.SafeRepresenter.add_representer(str, str_representer)

os.chdir(os.path.dirname(__file__))
for root, dirs, files in os.walk('workspaces'):
  for filename in files:
    if filename.endswith('.yaml'):
      with open(root + '/' + filename) as f:
        ws = yaml.safe_load(f)
      with open(root + '/' + filename, 'w') as f:
        yaml.safe_dump(ws, f, default_flow_style=False)
