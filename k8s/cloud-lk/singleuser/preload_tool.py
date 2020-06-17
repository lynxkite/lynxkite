#!/usr/bin/env python3
'''
This program has basically two uses. It can either sanity check the contents
of the preloaded_lk_data directory or it can generate the contents for
the preloaded_lk_data directory.

It both cases, it needs the workspaces directory.

Use case 1): Checking

./preload_tool.py --task=check --ws_dir workspaces --preloaded_dir preloaded_lk_data

This command will look at the files in directory "workspaces" and test if all
the necessary files for the initial import operations are there in the appropriate
places in directory "preloaded_lk_data".

These files are the following:
  1) Each imported file should be present in preloaded_lk_data/data/uploads/
     (e.g., preloaded_lk_data/data/uploads/fd368b1e3b00384564b2a69f3786343a.routes.csv)
  2) Each table should be present in preloaded_lk_data/data/tables
     (e.g., preloaded_lk_data/data/tables/42b5b660-f05a-36f5-bbbc-48e07e7c280b - this is actually a directory)
  3) The import meta operation should be present in preloaded_lk_data/meta/1/operations
     (e.g., preloaded_lk_data/meta/1/operations/save-1573026281708)
     This file should contain the operation guid (e.g., f62d902e-5d30-3e7e-888a-4ac8a56148ba)
  4) The operation success file should be present in preloaded_lk_data/data/operations/<guid>/_SUCCESS
     (e.g., preloaded_lk_data/data/operations/f62d902e-5d30-3e7e-888a-4ac8a56148ba/_SUCCESS)

If any of such files are missing, the preloaded_lk_data directory is incomplete.
The actual import is not tested, so this script can be fooled e.g., by putting an empty file
in the tables directory, but it does catch accidental errors.

Use case 2) Generating the preloaded_lk_data

./preload_tool.py --task=copy --ws_dir workspaces --preloaded_dir preloaded_lk_data --source_dir home/.kite/


This command will generate a basic preloaded_lk_data directory, by copying over the necessary files from
home/.kite. See the README.md file how this works.

'''
from ruamel import yaml
import json
import argparse
import glob
import os.path
from collections import namedtuple
import shutil


def get_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--ws_dir', type=str, required=True,
                      help='The path to the directory containing the yaml files to be uploaded')
  parser.add_argument('--preloaded_dir', type=str, required=True,
                      help='The path to preloaded_dir (which should contain directories "data" and "meta")')

  parser.add_argument('--source_dir', type=str, default='',
                      help='''The path to the source dir (which should be like the preloaded dir, so it
                      should contain directories "data" and "meta")''')

  parser.add_argument('--task', type=str, required=True,
                      help='''Set it either to "check" to check the contents of the preloaded directory, or "copy" to create
                      a preloaded directory. In the "copy" case, you also have to used the --source_dir flag
                      ''')

  return parser.parse_args()


def load_ws_from_yaml(filename):
  with open(filename) as f:
    ws = yaml.safe_load(f)
    return ws


def stripped(whole_string, prefix_to_be_disarded):
  assert whole_string.startswith(prefix_to_be_disarded)
  return whole_string[len(prefix_to_be_disarded):]


FileInfo = namedtuple('FileInfo', 'filename exists source')
TableAndUpload = namedtuple('TableAndUpload', 'table uploadfile')


def get_tables_and_uploads(ws_file):
  import_params = []
  for o in load_ws_from_yaml(ws_file):
    if o['operationId'].startswith('Import'):
      filename = o['parameters']['filename']
      filename = stripped(filename, 'UPLOAD$/')
      table = o['parameters']['imported_table']
      import_params.append(TableAndUpload(table=table, uploadfile=filename))
  return import_params


def get_fileinfo_list(ws_dir, metadata_dir):
  '''Returns a list of FileInfo's for each file that must be present.'''
  file_infos = []
  imported_table_to_guid = {}
  guid_to_meta_operation_file = {}

  for opfile in glob.glob(f'{metadata_dir}/meta/1/operations/*'):
    with open(opfile) as op:
      o = json.loads(op.read())
      if 'operation' in o:
        if 'class' in o['operation'] and o['operation']['class'].endswith('ImportDataFrame'):
          table = o['outputs']['t']
          guid = o['guid']
          imported_table_to_guid[table] = guid
          guid_to_meta_operation_file[guid] = stripped(opfile, metadata_dir)

  for y in glob.glob(f'{ARGS.ws_dir}/*'):
    for table, filename in get_tables_and_uploads(y):
      fi = FileInfo(filename=f'data/uploads/{filename}',
                    source=y,
                    exists=os.path.exists(f'{metadata_dir}/data/uploads/{filename}'))
      file_infos.append(fi)
      fi = FileInfo(filename=f'data/tables/{table}',
                    source=y,
                    exists=os.path.exists(f'{metadata_dir}/data/tables/{table}'))
      file_infos.append(fi)
      if table in imported_table_to_guid:
        guid = imported_table_to_guid[table]
        savefile = guid_to_meta_operation_file[guid]
        fi = FileInfo(filename=f'{savefile}',
                      source=y,
                      exists=os.path.exists(f'{metadata_dir}/{savefile}')
                      )
        file_infos.append(fi)
        fi = FileInfo(filename=f'data/operations/{guid}/_SUCCESS',
                      source=y,
                      exists=os.path.exists(f'{metadata_dir}/data/operations/{guid}/_SUCCESS'))
        file_infos.append(fi)
      else:
        # This is not an actual filename, but it will give the user an idea about
        # what is missing when the missing files are dumped.
        fi = FileInfo(filename=f'meta/1/operations/save-XXXXXXXXXX-import-for-table={table}',
                      source=y,
                      exists=False
                      )
        file_infos.append(fi)
  return file_infos


def copy(src_dir, dst_dir, relative_path):
  src = f'{src_dir}/{relative_path}'
  dst = f'{dst_dir}/{relative_path}'
  target_dir, target = os.path.split(dst)
  source_dir, source = os.path.split(src)
  os.makedirs(target_dir, exist_ok=True)
  if os.path.isdir(src):
    shutil.copytree(src, dst)
  else:
    shutil.copy(src, dst)


def checked(ws_dir, metadata_dir):
  items = get_fileinfo_list(ws_dir, metadata_dir)
  problems = [f for f in items if not f.exists]
  if len(problems) > 0:
    print(f'Missing files in {metadata_dir}')
    for p in problems:
      print(p)
    print('FAIL')
    print('''Maybe your preloaded_lk_data directory is out-of-date.
Use the newest version: s3://preloaded-lk-data/preloaded_lk_data.tgz

Or maybe you're creating new content here. Follow the instructions in README.md.
''')
    exit(1)
  return items


ARGS = get_args()
if ARGS.task == 'check':
  checked(ARGS.ws_dir, ARGS.preloaded_dir)
  exit(0)
elif ARGS.task == 'copy':
  if len(ARGS.source_dir) == 0:
    print('Please, set --source_dir')
    exit(1)
  jobs = checked(ARGS.ws_dir, ARGS.source_dir)
  for j in jobs:
    copy(ARGS.source_dir, ARGS.preloaded_dir, j.filename)
else:
  print(f'--task can only be set "check" or "copy", not "{ARGS.task}"')
  exit(1)
