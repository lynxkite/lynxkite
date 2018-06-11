'''Lists the currently referenced checkpoints from a given tags.journal file.'''
import json
import sys


def parse_tree(filename):
  db = {}
  with open(filename) as f:
    for line in f:
      if not line.strip():
        continue
      try:
        action, key, value = json.loads(line)
      except BaseException:
        pass  # LynxKite also ignores corrupt lines.
      if action == 'Put':
        db[key] = value
      elif action == 'Delete':
        del db[key]
      elif action == 'DeletePrefix':
        db = {k: v for (k, v) in db.items() if not k.startswith(key)}
  return db


def main(args):
  for k, v in parse_tree(args[1]).items():
    if k.endswith('/!checkpoint'):
      print(v)


if __name__ == '__main__':
  main(sys.argv)
