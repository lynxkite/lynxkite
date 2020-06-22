'''Tool for migrating workspaces from LynxKite 3.x to LynxKite 4.x.

The whole process would look like this:

  # Copy metadata, uploads, and imported tables. (Remote steps.)
  cp backup/meta/9/tags.journal kite/meta/1/tags.journal
  cp -R backup/meta/9/checkpoints kite/meta/1/checkpoints
  cp -R backup/data/uploads kite/data/uploads
  for f in `cd backup/meta/9/operations; grep -rl ImportDataFrame .`; do
    cp backup/meta/9/operations/$f kite/meta/1/operations/
  done
  for guid in `grep -r '"t"' kite/meta/1/operations | cut -d'"' -f4`; do
    cp -R backup/data/tables/$guid kite/data/tables/
  done
  for guid in `grep -r '"guid"' kite/meta/1/operations | cut -d'"' -f4`; do
    cp -R backup/data/operations/$guid kite/data/operations/
  done

  # Migrate checkpoints. (Local steps.)
  scp host:kite/meta/1/tags.journal .
  python migrate_to_lk4.py --tags tags.journal --list > cps
  scp host:cps .
  for f in `cat cps`; do
    scp host:kite/meta/1/checkpoints/$f checkpoints/
  done
  python migrate_to_lk4.py --tags tags.journal  > new_tags.journal
  scp -r new_checkpoints new_tags.journal host:
  # (Remote steps.)
  cp new_checkpoints/* kite/meta/1/checkpoints/
  cat new_tags.journal >> kite/meta/1/tags.journal

'''
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


def writenew(src, dst, **kwargs):
  with open(f'new_checkpoints/save-{dst}', 'w') as f:
    json.dump({
      **kwargs,
      'checkpoint': dst,
      'previousCheckpoint': src }, f)


def migrate(src, dst):
  if not os.path.exists(f'checkpoints/save-{src}'):
    return False
  with open(f'checkpoints/save-{src}') as f:
    j = json.load(f)
    if 'workspace' in j:
      writenew(src, dst, workspace=migrate_ws(j['workspace']))
      return True
    elif 'snapshot' in j:
      writenew(src, dst, snapshot=migrate_snapshot(j['snapshot']))
      return True
    else:
      return False


def migrate_snapshot(s):
  if s['kind'] == 'project':
    s['kind'] = 'graph'
  return s


def migrate_ws(ws):
  for b in ws['boxes']:
    # "project" -> "graph"
    inputs = b['inputs']
    if 'project' in inputs:
      inputs['graph'] = inputs['project']
      del inputs['project']
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
  return ws


if args.list:
  for v in sorted(checkpoints.values()):
    print(f'save-{v}')
else:
  t = int(time.time())
  for k, v in checkpoints.items():
    if migrate(v, str(t)):
      print(json.dumps(['Put', k, str(t)]))
      t += 1
