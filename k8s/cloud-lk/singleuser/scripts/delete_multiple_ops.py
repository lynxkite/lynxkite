#!/usr/bin/env python3
'''
Deletes any duplicate operation files from a meta operation directory. See
https://app.asana.com/0/153258440689361/1149619076199089 about
why this is necessary.

Example usage:
./delete_multiple_ops.py --folder /home/jovyan/.kite/meta/1/operations

'''
import json
import argparse
import os


def get_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--folder', type=str, required=True,
                      help='The name of the folder for the operation files')
  return parser.parse_args()


def get_guid_for_operation(filename):
  with open(filename) as f:
    ws = json.loads(f.read())
    return ws['guid']


ARGS = get_args()

# We want to make sure that it is the older operation (with the smaller timestamp)
# that gets preserved, so we sort here. Apparently, this is superfluous because
# listdir returns the files already sorted, but this behavior is not documented
# so we cannot rely on it.
files = sorted([f for f in os.listdir(ARGS.folder) if f.startswith('save-')])
ops = {}
for f in files:
  guid = get_guid_for_operation(os.path.join(ARGS.folder, f))
  if guid in ops:
    dup = os.path.join(ARGS.folder, f)
    orig = os.path.join(ARGS.folder, ops[guid])
    print(f'Guid {guid} tells us that file {dup} is a duplicate of: {orig}')
    os.remove(dup)
  else:
    ops[guid] = f
