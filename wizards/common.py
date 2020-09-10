import os
from ruamel import yaml
import lynx.kite

def str_representer(dumper, s):
  if '\n' in s:
    style = '|'
  elif len(s) > 60:
    style = '>'
  else:
    style = None
  return dumper.represent_scalar('tag:yaml.org,2002:str', s, style=style)


yaml.representer.SafeRepresenter.add_representer(str, str_representer)


def ws_name(filename):
  filename, _ = os.path.splitext(filename)
  return filename.replace('_', ' ').replace('custom boxes/', 'custom_boxes/')

def filename(ws_name):
  return ws_name.replace(' ', '_') + '.yaml'

def get_lk(mode):
  if mode == '--remote':
    with open(os.path.expanduser('~/secrets/try.lynxkite.com.password')) as f:
      password = f.read().strip()
      lk = lynx.kite.LynxKite(
          address='https://try.lynxkite.com/',
          username='admin',
          password=password)
      lk._login()
      return lk
  elif mode == '--local':
    return lynx.kite.LynxKite()
  else:
    assert False, f'Unknown lynxkite mode: {mode}'
