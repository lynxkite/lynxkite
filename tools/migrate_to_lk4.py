'''Tool for migrating workspaces from LynxKite 3.x to LynxKite 4.x.'''
import argparse
import json
import os
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--tags', help='path to tags.journal')
parser.add_argument('--list', action='store_true', help='just list the checkpoint files')
args = parser.parse_args()
checkpoints = {}
with open(args.tags) as f:
  for line in f:
    if not line.strip():
      continue
    try:
      j = json.loads(line)
    except:
      continue
    if not j[1].endswith('/!checkpoint'):
      continue
    if j[0] == 'Put' and j[2]:
      checkpoints[j[1]] = j[2]
    elif j[0] == 'Delete' and j[1] in checkpoints:
      del checkpoints[j[1]]

MAPPING = {
    'Convert edge attribute to Double': 'Convert edge attribute to number',
    'Convert vertex attribute to Double': 'Convert vertex attribute to number',
    'Convert vertex attributes to position': 'Bundle vertex attributes into a Vector',
    'Copy edges to base project': 'Copy edges to base graph',
    'Copy scalar': 'Copy graph attribute',
    'Copy scalar from other project': 'Copy graph attribute from other graph',
    'Derive scalar': 'Derive graph attribute',
    'Discard scalars': 'Discard graph attributes',
    'Embed with t-SNE': 'Reduce attribute dimensions',
    'Project rejoin': 'Graph rejoin',
    'Project union': 'Graph union',
    'Link project and segmentation by fingerprint': 'Link base graph and segmentation by fingerprint',
    'Predict attribute by viral modeling': 'built-ins/Predict from communities',
    'Reduce vertex attributes to two dimensions': 'Reduce attribute dimensions',
    'Rename scalar': 'Rename graph attributes',
    'Segment by Double attribute': 'Segment by numeric attribute',
    'Set scalar icon': 'Set graph attribute icon',
    'Take segmentation as base project': 'Take segmentation as base graph',
    'Take segmentation links as base project': 'Take segmentation links as base graph',
    'Use base project as segmentation': 'Use base graph as segmentation',
    'Use other project as segmentation': 'Use other graph as segmentation',
    'built-ins/bar-chart': 'built-ins/Bar chart',
    'built-ins/graph-metrics': 'built-ins/Graph metrics',
    'built-ins/line-chart': 'built-ins/Line chart',
    'built-ins/scatter-plot': 'built-ins/Scatter plot',
    'built-ins/stacked-bar-chart': 'built-ins/Stacked bar chart',
  }


def migrate(src, dst):
  if not os.path.exists(f'checkpoints/save-{src}'):
    return False
  with open(f'checkpoints/save-{src}') as f:
    j = json.load(f)
    if 'workspace' not in j:
      return False
    ws = j['workspace']
  for b in ws['boxes']:
    # "project" -> "graph"
    inputs = b['inputs']
    if 'project' in inputs:
      inputs['graph'] = inputs['project']
    for i in inputs.values():
      if i['id'] == 'project':
        i['id'] = 'graph'
    # Renamed operations.
    b['operationId'] = MAPPING.get(b['operationId'], b['operationId'])
    parameters = b['parameters']
    # Some automatic fixes.
    if b['operationId'] == 'Copy graph attribute from other graph':
      if 'scalar' in inputs:
        inputs['source'] = inputs['scalar']
        del inputs['scalar']
      if 'graph' in inputs:
        inputs['destination'] = inputs['graph']
        del inputs['graph']
    if b['operationId'] == 'Bundle vertex attributes into a Vector':
      parameters['output'] = parameters.get('output', 'position')
      if 'x' in parameters and 'y' in parameters:
        parameters['elements'] = parameters['x'] + ',' + parameters['y']
        del parameters['x']
        del parameters['y']
    if b['operationId'] == 'Rename graph attributes':
      if 'before' in parameters and 'after' in parameters:
        parameters['change_' + parameters['before']] = parameters['after']
        del parameters['before']
        del parameters['after']
  with open(f'new_checkpoints/save-{dst}', 'w') as f:
    json.dump({
      'workspace': ws,
      'checkpoint': dst,
      'previousCheckpoint': src }, f)
  return True


if args.list:
  for v in sorted(checkpoints.values()):
    print(f'save-{v}')
else:
  t = int(time.time())
  for k, v in checkpoints.items():
    if migrate(v, str(t)):
      print(json.dumps(['Put', k, str(t)]))
      t += 1
