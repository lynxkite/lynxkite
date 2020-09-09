#!/usr/bin/env python3
'''Downloads a workspace and its subworkspaces to the "workspaces" directory
from try.lynxkite.com or localhost.'''
import lynx.kite
import os
from ruamel import yaml
import sys
from common import ws_name, filename, get_lk

def download(fn, saved=set()):
  if fn in saved:
    return
  wn = ws_name(fn)
  ws = lk.get_workspace(wn)
  boxes = ws.workspace.boxes
  full_fn = 'workspaces/' + fn
  print(f'Saving to: {full_fn}')
  directory = os.path.dirname(full_fn)
  if not os.path.exists(directory):
    os.makedirs(directory)
  with open(full_fn, 'w') as f:
    yaml.safe_dump(lynx.kite.to_simple_dicts(boxes), f, default_flow_style=False)
  saved.add(fn)
  for b in boxes:
    if b.operationId.find('/') >= 0:
        download(filename(b.operationId), saved)

def usage():
  print('Usage:')
  print()
  print('download_ws.py --remote|--local file_name')
  print()
  print('--remote downloads from https://try.lynxkite.com')
  print('--local  downloads from your locally running lynxkite (set your LYNXKITE_ADDRESS)')
  print()
  sys.exit(0)

os.chdir(os.path.dirname(__file__))

if len(sys.argv) != 3:
  usage()

lk = get_lk(sys.argv[1])

download(sys.argv[2])
