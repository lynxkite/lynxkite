# GENERATED FILE, DO NOT EDIT!!!
import os
import subprocess


def _parse_version(filename):
  with open(filename, "r") as f:
    from packaging import version
    v = version.parse(f.read().strip())
    if isinstance(v, version.LegacyVersion):
      return '0.0.0+badversion'
    return str(v)


def _try_to_get_version_from_git():
  try:
    url = subprocess.check_output(
        ['git', 'config', '--get', 'remote.origin.url']).decode('utf-8').strip()
    if url == 'git@github.com:biggraph/biggraph.git':
      v = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode('utf-8').strip()
      return f'0.0.0+dev_{v}'
    else:
      return '0.0.0+unknown'
  except BaseException:
    return '0.0.0+unknown2'


def version():
  dir = os.path.dirname(__file__)
  filename = os.path.join(dir, 'version')
  if os.path.isfile(filename):
    return _parse_version(filename)
  return _try_to_get_version_from_git()
