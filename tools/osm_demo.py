#!/usr/bin/env python3
'''
Given an open street map xml input, this tool will generate two csv files that can be used to demonstrate
the Steiner tree computation of LynxKite. Example usage:

  ./osm_demo.py --input bp.osm  --total_rewards 0.05 --num_rewards 5 --num_access_points 5 --seed 23443551

An example workspace that uses the output is here:
https://pizzabox.lynxanalytics.com/#/workspace/Users/gabor.olah@lynxanalytics.com/steinerdemo

'''
import random
import math
import untangle
import argparse

from collections import defaultdict
from collections import deque


def get_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--input', type=str, required=True,
                      help='The path to the input xml file (osm format)')

  parser.add_argument('--vertex_csv', type=str, default='vertices.csv',
                      help='''The name of the output vertex csv file''')

  parser.add_argument('--edge_csv', type=str, default='edges.csv',
                      help='''The name of the output edge csv file''')

  parser.add_argument('--random_cost_low', type=float, default=0.5,
                      help='''Edge costs fall in the interval [random_cost_low, random_cost_high]*edge_length''')

  parser.add_argument('--random_cost_high', type=float, default=1.5,
                      help='''Edge costs fall in the interval [random_cost_low, random_cost_high]*edge_length''')

  parser.add_argument('--seed', type=float, default=424242,
                      help='''Seed for random numbers''')

  parser.add_argument('--num_access_points', type=int, default=1,
                      help='''Number of access points''')

  parser.add_argument('--num_rewards', type=int, default=5,
                      help='''Number of vertices with rewards''')

  parser.add_argument('--total_rewards', type=float, default=0.5,
                      help='''Total sum of rewards''')
  return parser.parse_args()


ARGS = get_args()

VERTICES = set()
POS = {}
AP = set()
REV = {}
EDGES = {}
TOTAL_COSTS = 0.0

random.seed(ARGS.seed)


def dist(src_id, dst_id):
  dx = POS[src_id][0] - POS[dst_id][0]
  dy = POS[src_id][1] - POS[dst_id][1]
  return math.hypot(dx, dy)


def random_cost(src_id, dst_id):
  return random.uniform(ARGS.random_cost_low, ARGS.random_cost_high) * dist(src_id, dst_id)


def toid(s):
  'Return string-like ids'
  return 'id_' + s


def parse_xml_and_add_costs():
  xml = untangle.parse(ARGS.input)
  for n in xml.osm.node:
    id = toid(n['id'])
    pos = (float(n['lat']), float(n['lon']))
    POS[id] = pos

  edges = set()
  for w in xml.osm.way:
    if hasattr(w, 'tag') and 'highway' in [m['k'] for m in w.tag]:
      nodes = w.nd
      for i in range(len(nodes) - 1):
        src = toid(nodes[i]['ref'])
        dst = toid(nodes[i + 1]['ref'])
        edge = (src, dst)
        edges.add(edge)
        VERTICES.add(src)
        VERTICES.add(dst)

  for e in edges:
    c = random_cost(e[0], e[1])
    global TOTAL_COSTS
    TOTAL_COSTS += c
    EDGES[(e[0], e[1])] = c
    EDGES[(e[1], e[0])] = c


def only_keep_greatest_connected_component():
  graph = defaultdict(list)
  for e in EDGES:
    graph[e[0]].append(e[1])

  greatest_component = set()
  while len(graph) > 0:
    d = deque()
    some_vertex = next(iter(graph))
    d.append(some_vertex)
    reachable = set()
    while len(d) > 0:
      src = d.popleft()
      reachable.add(src)
      for dst in graph[src]:
        if dst not in reachable:
          d.append(dst)
    for r in reachable:
      del graph[r]
    if (len(reachable) > len(greatest_component)):
      greatest_component = reachable

  global VERTICES
  VERTICES = greatest_component


def setup_ap_and_rev():
  ids = list(VERTICES).copy()
  random.shuffle(ids)
  for i in range(ARGS.num_access_points):
    AP.add(ids[i])
  for i in range(ARGS.num_rewards):
    REV[ids[ARGS.num_access_points + i]] = ARGS.total_rewards / ARGS.num_rewards


parse_xml_and_add_costs()
only_keep_greatest_connected_component()
setup_ap_and_rev()


with open(ARGS.vertex_csv, 'w') as f:
  f.write('id,lat,lon,revenue,ap\n')
  for id in VERTICES:
    lat = POS[id][0]
    lon = POS[id][1]
    rev = str(REV[id]) if id in REV else '0.0'
    ap = '0.0' if id in AP else ''
    f.write(f'{id},{lat},{lon},{rev},{ap}\n')

with open(ARGS.edge_csv, 'w') as f:
  f.write('src,dst,cost\n')
  for v in EDGES:
    if (v[0] in VERTICES):
      assert (v[1] in VERTICES)
      f.write(f'{v[0]},{v[1]},{EDGES[v]:.14f}\n')


print(f'Total costs on edges: {TOTAL_COSTS}')
