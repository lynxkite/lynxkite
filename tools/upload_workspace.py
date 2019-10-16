#!/usr/bin/env python3
'''
This script reads in a yaml representation of a workspace and uploads
it to LynxKite. The yaml representation can be obtained, e.g., by selecting
the workspace of a LK and then copying it to the clipboard.

'''
import yaml
import argparse
import lynx.kite

def get_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--folder', type=str, default='Demos',
                      help='The name of the folder in which the workspace is to be created')

  parser.add_argument('--ws_name', type=str, required=True,
                      help='The name of the workspace in LynxKite')

  parser.add_argument('--ws_file', type=str, required=True,
                      help='The path to the yaml file that represents the workspace to be uploaded')

  return parser.parse_args()


def load_ws_from_yaml(filename):
    with open(filename) as f:
        return yaml.load(f, Loader=yaml.FullLoader)


ARGS = get_args()
LK = lynx.kite.LynxKite()
try:
  LK.create_dir(ARGS.folder)
except lynx.kite.LynxException as e:
  if not 'already exists' in e.error:
    raise
LK.save_workspace(f'{ARGS.folder}/{ARGS.ws_name}', load_ws_from_yaml(ARGS.ws_file))
