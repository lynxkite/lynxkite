"""Deletes an operation and all the operations that use its outputs."""
import json
import os
import sys

def main():
  if len(sys.argv) != 2:
    print 'Usage:'
    print '  python rmoperation.py <operation save file>'
    sys.exit(1)

  fn = sys.argv[1]
  j = read(fn)
  tainted = delete(fn, j)

  opdir = os.path.dirname(fn)
  for fn in sorted(os.listdir(opdir)):
    fn = os.path.join(opdir, fn)
    j = read(fn)
    if tainted.intersection(j['inputs'].values()):
      tainted |= delete(fn, j)

def read(fn):
  with file(fn) as f:
    return json.load(f)

def delete(fn, j):
  print 'deleting', fn, j['operation']['class']
  os.remove(fn)
  return set(j['outputs'].values())

if __name__ == '__main__':
  main()
