#!/usr/bin/env python3
import yaml
import requests
import argparse

'''
This script reads in a yaml representation of a workspace and uploads
it to LynxKite. The yaml representation can be obtained, e.g., by selecting
the workspace of a LK and then copying it to the clipboard.

'''

def get_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--address', type=str, default='http://localhost:2200',
                      help='The address and port of the LynxKite instance we are connecting to.')
  parser.add_argument('--folder', type=str, default='Demos',
                      help='The name of the folder in which the workspace is to be created')

  parser.add_argument('--ws_name', type=str, required=True,
                      help='The name of the workspace in LynxKite')

  parser.add_argument('--ws_file', type=str, required=True,
                      help='The path to the yaml file that represents the workspace to be uploaded')

  return parser.parse_args()


ARGS = get_args()

def load_ws_from_yaml(filename):
    with open(filename) as f:
        return yaml.load(f, Loader=yaml.FullLoader)


folder=ARGS.folder
workspace_name=ARGS.ws_name
address=ARGS.address

# Create directory, ignore error (if it exists)
data = {'name': folder, 'privacy': 'public-read'}
requests.post(f'{address}/ajax/createDirectory', json=data)

# Create workspace, ignore error (if if exists)
data = {'name': f'{folder}/{workspace_name}'}
requests.post(f'{address}/ajax/createWorkspace', json=data)

# Set the workspace
data = {
    'reference':  {'top': f'{folder}/{workspace_name}', 'customBoxStack': [] },
    'workspace': {'boxes': load_ws_from_yaml(ARGS.ws_file)}
}
r = requests.post(f'{address}/ajax/setAndGetWorkspace', json=data)
r.raise_for_status()

