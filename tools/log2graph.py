#!/usr/bin/env python3
'''
Parses a LynxKite logfile and dumps the critical path from the first executed backend operation
to the last created guid. Example usage:

./log2graph.py < application-20200318_101213.log

Excerpt from a possible output:

0 c6475919-831a-34ee-93fc-0a3631ac8ba4[OrderedSphynxDisk] -> DerivePython_start_0
5016 DerivePython_start_0 -> DerivePython_end_0  DerivePython(matches = vs[vs.name.str.lower() ==...
0 DerivePython_end_0 -> 99250d74-4e81-3f36-a02b-2a850bfcabce[OrderedSphynxDisk]
36 99250d74-4e81-3f36-a02b-2a850bfcabce[OrderedSphynxDisk] -> 99250d74-4e81-3f36-a02b-2a850bfcabce[SphynxMemory]
0 99250d74-4e81-3f36-a02b-2a850bfcabce[SphynxMemory] -> ShortestPath_start_1
761 ShortestPath_start_1 -> ShortestPath_end_1  ShortestPath(10.0)
0 ShortestPath_end_1 -> 100b63fe-e164-3a1f-ae74-f81777d68a14[SphynxMemory]
91 100b63fe-e164-3a1f-ae74-f81777d68a14[SphynxMemory] -> 100b63fe-e164-3a1f-ae74-f81777d68a14[OrderedSphynxDisk]
0 100b63fe-e164-3a1f-ae74-f81777d68a14[OrderedSphynxDisk] -> DerivePython_start_2
...
0 b773db8a-85bb-3fca-933f-db691f84b4fd[SparkDomain] -> CollectAttribute_start_0
69 CollectAttribute_start_0 -> CollectAttribute_end_0  CollectAttribute(...))
0 CollectAttribute_end_0 -> ce8b35fb-409b-3b33-8f56-10f44112a869[SparkDomain]
16850 TOTAL

Explanation:
1. Guid c6475919-831a-34ee-93fc-0a3631ac8ba4[OrderedSphynxDisk] is input to DerivePython
   (no time is charged here).
2. DerivePython runs in 5016 ms.
3. It's output  99250d74-4e81-3f36-a02b-2a850bfcabce is on OrderedSphynxDisk for in no time.
4. In 36 ms, the guid 99250d74-4e81-3f36-a02b-2a850bfcabce is relocated to SphynxMemory
5. Free of charge, 99250d74-4e81-3f36-a02b-2a850bfcabce is input by ShortestPath
6. ShortestPath finished in 761 ms
7. Its output 100b63fe-e164-3a1f-ae74-f81777d68a14 goes to SphynxMemory, 0 ms.
8. In 91 ms, 100b63fe-e164-3a1f-ae74-f81777d68a14 goes to OrderedSphynxDisk
9. And DerivePython can start working
...
In the end, we see some SparkDomain operations for visualization.
The last line is the total time taken. Probably there is some overhead, because because it is
always noticeably less than the total length of the computation. (E.g., in the example above,
the actual time was 22 seconds, but this path was only 17 seconds long.)

TODO: Dump the whole graph for LK to import.
'''

import fileinput
import re
from collections import defaultdict
import datetime

GRAPH = defaultdict(list)
OPS = defaultdict(int)
FULLOP = {}


def add_edge(v1, v2, cost):
  w = (v2, cost)
  GRAPH[v1].append(w)


def guid_in_domain(guid, domain):
  return f'{guid}[{domain}]'


regexp_common = re.compile(
    r'^I(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d).*elapsed: (\d+) (OPERATION_LOGGER_MARKER|RELOCATION_LOGGER_MARKER) (.*)')
regexp_rel = re.compile('Moving ([^ ]+) from ([^ ]+) to ([^ ]+)')
regexp_op = re.compile('([^ ]+) opguid: ([^ ]+) inputs: ([^ ]+) outputs: ([^ ]+) op: (.*)')


def extract_common(line):
  m = regexp_common.match(line)
  dt = datetime.datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S,%f')
  elapsed = int(m.group(2))
  return elapsed, m.group(3), m.group(4)


def op(line):
  ms, _, rest = extract_common(line)
  m = regexp_op.match(rest)
  domain = m.group(1)
  opguid = m.group(2)
  inputs = m.group(3)
  inputs = inputs[1:len(inputs) - 1].split(',')
  outputs = m.group(4)
  outputs = outputs[1:len(outputs) - 1].split(',')
  op = m.group(5)

  if 'com.lynxanalytics.biggraph.graph_operations.ImportDataFrame' in op:
    op = 'ImportDataFrame()'
  idx = op.find('(')
  op = op[:idx]
  opid = OPS[op]
  OPS[op] = opid + 1
  op_start = f'{op}_start_{opid}'
  FULLOP[op_start] = m.group(5)
  op_end = f'{op}_end_{opid}'
  add_edge(op_start, op_end, ms)
  for i in inputs:
    if i:
      add_edge(guid_in_domain(i, domain), op_start, 0)
  for o in outputs:
    if o:
      add_edge(op_end, guid_in_domain(o, domain), 0)


def rel(line):
  ms, _, rest = extract_common(line)
  m = regexp_rel.match(rest)
  guid = m.group(1)
  src = m.group(2)
  dst = m.group(3)
  g1 = guid_in_domain(guid, src)
  g2 = guid_in_domain(guid, dst)
  add_edge(g1, g2, ms)


for line in fileinput.input():
  line = line.rstrip()
  if 'OPERATION_LOGGER_MARKER' in line:
    op(line)
  elif 'RELOCATION_LOGGER_MARKER' in line:
    rel(line)

# Add a source and a sink none
VERTICES = set()
SOURCES = set()
TARGETS = set()
for v in GRAPH:
  VERTICES.add(v)
  for l in GRAPH[v]:
    SOURCES.add(v)
    vv = l[0]
    VERTICES.add(vv)
    TARGETS.add(vv)

for v in VERTICES - TARGETS:
  add_edge('START', v, 0)

for v in VERTICES - SOURCES:
  add_edge(v, 'END', 0)

# A simple critical path implementation.
# Use compute shortest path on graph, with the
# edge weights negated. This would not work with Dykstra, but
# does work with Bellman-Ford, since this graph is a DAG.

DISTANCE = {}
PRED = {}
for v in VERTICES:
  DISTANCE[v] = float('inf')
  PRED[v] = None

DISTANCE['START'] = 0
DISTANCE['END'] = float('inf')
TIMES = {}
EDGES = []
for v in GRAPH:
  for l in GRAPH[v]:
    vv = l[0]
    w = l[1]
    e = (v, vv, -w)
    TIMES[f'{v}->{vv}'] = w
    EDGES.append(e)


for _ in range(len(VERTICES)):
  for e in EDGES:
    src = e[0]
    dst = e[1]
    w = e[2]
    if DISTANCE[src] + w < DISTANCE[dst]:
      DISTANCE[dst] = DISTANCE[src] + w
      PRED[dst] = src


v = 'END'
total_time = 0
shpath = []
while PRED[PRED[v]] != 'START':
  src = PRED[PRED[v]]
  dst = PRED[v]
  estr = f'{src}->{dst}'
  full = ''
  if src in FULLOP:
    full = FULLOP[src]
  total_time += TIMES[estr]
  shpath.append(f'{TIMES[estr]} {src} -> {dst}  {full}')
  v = PRED[v]

for e in shpath[::-1]:
  print(e)
print(total_time, 'TOTAL')
