#!/usr/bin/env python3
'''
Given an open street map xml input, this tool will generate two csv files that can be used to demonstrate
the Steiner tree computation of LynxKite. Example usage:

  ./osm_demo.py --input budapestp.osm  --total_rewards 0.05 --num_rewards 5 --num_access_points 5 --seed 23443551

An example workspace that uses the output is here:
https://pizzabox.lynxanalytics.com/#/workspace/Users/gabor.olah@lynxanalytics.com/steinerdemo
The total cost is printed at the end, so you can set --total_rewards appropriately and re-run the script if
you see fit.

You can also discard some nodes: this is useful if you want to show the full map on the demo.
The node_discard_threshold parameter can be used to fine tune this:
Any node that has only two neighbors and is closer to either of its neighbors then this threshold
is dropped, and the two neigbors are connected with a new arc. At the end of the run, the total number
or vertices and edges are also printed: LynxKite cannot visualize more than 5000 edges, so you will
know if you need to re-run the script.

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

  parser.add_argument('--node_discard_threshold', type=float, default=-1.0,
                      help='''Discard a degree 2 node whose distance to either of its neighbors is smaller than this''')
  return parser.parse_args()


def dist(positions, src_id, dst_id):
  dx = positions[src_id][0] - positions[dst_id][0]
  dy = positions[src_id][1] - positions[dst_id][1]
  return math.hypot(dx, dy)


def random_cost(positions, random_cost_low, random_cost_high, src_id, dst_id):
  return random.uniform(random_cost_low, random_cost_high) * dist(positions, src_id, dst_id)


def toid(s):
  '''Return string-like ids'''
  return 'id_' + s


def get_positions(xml):
  positions = {}
  for n in xml.osm.node:
    id = toid(n['id'])
    pos = (float(n['lat']), float(n['lon']))
    positions[id] = pos
  return positions


def get_graph(xml):
  graph = defaultdict(set)

  for w in xml.osm.way:
    if hasattr(w, 'tag') and 'highway' in [m['k'] for m in w.tag]:
      nodes = w.nd
      for i in range(len(nodes) - 1):
        src = toid(nodes[i]['ref'])
        dst = toid(nodes[i + 1]['ref'])
        graph[src].add(dst)
        graph[dst].add(src)
  return graph


def get_greatest_component(bigger_graph):
  graph = bigger_graph.copy()
  greatest_component = set()
  while graph:
    d = deque()
    some_vertex = next(iter(graph))
    d.append(some_vertex)
    reachable = set()
    while d:
      src = d.popleft()
      reachable.add(src)
      for dst in graph[src]:
        if dst not in reachable:
          d.append(dst)
    for r in reachable:
      del graph[r]
    if len(reachable) > len(greatest_component):
      greatest_component = reachable

  subgraph = defaultdict(set)
  for src in greatest_component:
    for dst in bigger_graph[src]:
      if dst in greatest_component:
        subgraph[src].add(dst)

  return subgraph


def get_vertices(graph):
  vertices = set()
  for src in graph.keys():
    vertices.add(src)
    for dst in graph[src]:
      vertices.add(dst)
  return vertices


def get_access_and_reward_nodes(args, vertices):
  ids = list(vertices)
  access_nodes = set()
  reward_nodes = set()
  random.shuffle(ids)
  for i in range(args.num_access_points):
    access_nodes.add(ids[i])
  for i in range(args.num_rewards):
    reward_nodes.add(ids[args.num_access_points + i])
  return (access_nodes, reward_nodes)


def discard_too_dense_nodes(position, args, graph):
  more_to_go = True
  while more_to_go:
    more_to_go = False
    vertices = get_vertices(graph)
    for node in vertices:
      if len(graph[node]) == 2:
        l = list(graph[node])
        prev = l[0]
        nxt = l[1]
        if dist(position, prev, node) < args.node_discard_threshold or dist(
                position, node, nxt) < args.node_discard_threshold:
          del graph[node]
          more_to_go = True
          graph[prev].add(nxt)
          graph[prev].remove(node)
          graph[nxt].remove(node)
          graph[nxt].add(prev)


def main():
  args = get_args()
  random.seed(args.seed)
  xml = untangle.parse(args.input)
  positions = get_positions(xml)
  full_graph = get_graph(xml)
  graph = get_greatest_component(full_graph)
  discard_too_dense_nodes(positions, args, graph)

  vertices = get_vertices(graph)
  access_nodes, reward_nodes = get_access_and_reward_nodes(args, vertices)

  with open(args.vertex_csv, 'w') as f:
    f.write('id,lat,lon,revenue,ap\n')
    for id in vertices:
      lat = positions[id][0]
      lon = positions[id][1]
      rev = str(args.total_rewards / args.num_rewards) if id in reward_nodes else '0.0'
      ap = '0.0' if id in access_nodes else ''
      f.write(f'{id},{lat},{lon},{rev},{ap}\n')

  with open(args.edge_csv, 'w') as f:
    costs_assigned = {}
    total_cost = 0.0
    f.write('src,dst,cost\n')
    for src in graph.keys():
      if (src in vertices):
        for dst in graph[src]:
          assert (dst in vertices)
          if (src, dst) not in costs_assigned:
            cost = random_cost(positions, args.random_cost_low, args.random_cost_high, src, dst)
            total_cost += cost
            costs_assigned[(src, dst)] = cost
            costs_assigned[(dst, src)] = cost
          f.write(f'{src},{dst},{costs_assigned[(src,dst)]:.14f}\n')
  num_edges = 0
  for w in graph:
    num_edges += len(graph[w])
  print(f'Total costs on edges: {total_cost}, vertices: {len(vertices)} edges: {num_edges}')


main()
