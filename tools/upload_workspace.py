#!/usr/bin/env python3
'''
This script reads in a yaml representation of a workspace and uploads
it to LynxKite. The yaml representation can be obtained, e.g., by selecting
the workspace of a LK and then copying it to the clipboard. We make sure
that there is exactly one anchor box
(see https://app.asana.com/0/search/1145664077193860/1136708269731017).
'''
from ruamel import yaml
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


def anchor():
  return yaml.safe_load('''
id: anchor
operationId: Anchor
parameters: {}
x: 0
y: 0
inputs: {}
parametricParameters: {}
  ''')


def load_ws_from_yaml_with_exactly_one_anchor(filename):
  with open(filename) as f:
    ws = yaml.safe_load(f)
    if len([x for x in ws if x['id'] == 'anchor']) != 1:
      ws = [x for x in ws if x['id'] != 'anchor']
      ws.insert(0, anchor())
    return ws


ARGS = get_args()

LK = lynx.kite.LynxKite()
try:
  LK.create_dir(ARGS.folder)
except lynx.kite.LynxException as e:
  if not 'already exists' in e.error:
    raise
LK.save_workspace(
    f'{ARGS.folder}/{ARGS.ws_name}',
    load_ws_from_yaml_with_exactly_one_anchor(
        ARGS.ws_file))
