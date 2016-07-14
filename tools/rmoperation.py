"""Deletes an operation and all the operations that use its outputs."""
import json
import os
import sys


def main():
  if len(sys.argv) < 2:
    print 'Usage:'
    print '  python rmoperation.py <operation save files>'
    sys.exit(1)

  orphans = set()  # GUIDs which cannot be calculated anymore.
  for fn in sys.argv[1:]:
    j = read(fn)
    orphans |= delete(fn, j)

  opdir = os.path.dirname(fn)
  for fn in sorted(os.listdir(opdir)):
    fn = os.path.join(opdir, fn)
    j = read(fn)
    if orphans.intersection(j['inputs'].values()):
      orphans |= delete(fn, j)


def read(fn):
  with file(fn) as f:
    return json.load(f)


def delete(fn, j):
  print 'deleting', fn, j['operation']['class']
  os.remove(fn)
  return set(j['outputs'].values())

if __name__ == '__main__':
  main()
