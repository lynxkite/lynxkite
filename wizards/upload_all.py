#!/usr/bin/env python3
'''Uploads the workspaces from the "workspaces" directory to try.lynxkite.com.'''
import lynx.kite
import os
from ruamel import yaml
import sys


def str_representer(dumper, s):
  if '\n' in s:
    style = '|'
  elif len(s) > 60:
    style = '>'
  else:
    style = None
  return dumper.represent_scalar('tag:yaml.org,2002:str', s, style=style)


yaml.representer.SafeRepresenter.add_representer(str, str_representer)


def fix_imports(filename, rewrite):
  wn = ws_name(filename)
  ws = lk.get_workspace(wn)
  boxes = ws.workspace.boxes
  outputs = {o.boxOutput.boxId: o for o in ws.outputs}
  for b in boxes:
    if b.operationId == 'Import CSV':
      fn = b.parameters.filename
      if fn and outputs[b.id].kind == 'error':
        _, fn = fn.split('.', 1)
        print(f'Uploading {fn} for {b.id} in {wn}.')
        with open('data/' + fn) as f:
          data = f.read()
        b.parameters.filename = lk.upload(data, fn)
        boxes = lk.import_box(boxes, b.id)
  lk.save_workspace(wn, boxes)
  if rewrite:
    with open('workspaces/' + filename, 'w') as f:
      yaml.safe_dump(lynx.kite.to_simple_dicts(boxes), f, default_flow_style=False)


def mkdir(dn):
  if dn:
    try:
      lk.create_dir(dn)
    except lynx.kite.LynxException as e:
      if not 'already exists' in e.error:
        raise e


def ws_name(filename):
  filename, _ = os.path.splitext(filename)
  return filename.replace('_', ' ').replace('custom boxes/', 'custom_boxes/')


def upload(filename, rewrite):
  print('Uploading', filename)
  with open('workspaces/' + filename) as f:
    ws = yaml.safe_load(f)
  wn = ws_name(filename)
  dn, _ = os.path.split(wn)
  mkdir(dn)
  lk.save_workspace(wn, ws)
  fix_imports(filename, rewrite)

def remove_guids(ws):
  for b in ws:
    if b['operationId'] == 'Import CSV':
      b['parameters']['imported_table'] = ''

  
def needs_upload(filename, rewrite):
  wn = ws_name(filename)
  entry = lk.get_directory_entry(wn)
  if not entry.exists:
    print(f'New workspace {wn}')
    return True
  if not entry.isWorkspace:
    print(f'Trying to override non-workspace: {wn}!')
    sys.exit(-1)
  with open('workspaces/' + filename) as f:
    ws_new = yaml.safe_load(f)
    if not rewrite:
      remove_guids(ws_new)
  ws_old = lynx.kite.to_simple_dicts(lk.get_workspace_boxes(wn))
  if not rewrite:
    remove_guids(ws_old)
  if ws_old != ws_new:
    print(f'Modified workspace {wn}')
    fn = f'workspaces/{filename}.orig'
    with open(fn, 'w') as f:
      yaml.safe_dump(ws_old, f, default_flow_style=False)
    print(f'Current server version saved as {fn}')
    return True
  return False

def usage():
  print('Usage:')
  print()
  print('upload_all.py [--remote|--local]')
  print()
  print('--remote uploads to https://try.lynxkite.com')
  print('--local  uploads to your locally running lynxkite (set your LYNXKITE_ADDRESS)')
  print()
  sys.exit(0)


os.chdir(os.path.dirname(__file__))

if len(sys.argv) != 2:
  usage()

if sys.argv[1] == '--remote':
  with open(os.path.expanduser('~/secrets/try.lynxkite.com.password')) as f:
    password = f.read().strip()
    lk = lynx.kite.LynxKite(
        address='https://try.lynxkite.com/',
        username='admin',
        password=password)
    lk._login()
elif sys.argv[1] == '--local':
  lk = lynx.kite.LynxKite()
else:
  usage()


to_upload = []
rewrite = sys.argv[1] == '--remote'

for root, dirs, files in os.walk('workspaces'):
  for f in files:
    if f.endswith('.yaml'):
      fn = (root + '/' + f)[len('workspaces/'):]
      if needs_upload(fn, rewrite):
        to_upload.append(fn)

if to_upload:
  input("Press Enter to continue or Ctrl-C to exit...")
  for fn in to_upload:
    upload(fn, rewrite)
else:
  print('Nothing new to upload')
