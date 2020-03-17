#!/usr/bin/env python3
'''
Parses a LynxKite log file (from stdin) and dumps a performance measurement summary based
on the loglines that contain either the word OPERATION_LOGGER_MARKER or RELOCATION_LOGGER_MARKER
'''

import fileinput
import re
from collections import defaultdict
import datetime

GRAPH = defaultdict(list)
OPS = defaultdict(int)


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
#  print ('LINE:', line)
#  print ('OUTPUTS:', outputs)
  op = m.group(5)
    
  if 'com.lynxanalytics.biggraph.graph_operations.ImportDataFrame' in op:
    op = 'ImportDataFrame()'
  idx = op.find('(')
  op = op[:idx]
  opid = OPS[op]
  OPS[op] = opid + 1
  op_start = f'{op}_start_{opid}'
  op_end = f'{op}_end_{opid}'
  add_edge(op_start, op_end, ms)
  for i in inputs:
    if i:
#      print (f'OPI: [{i}] LINE: {line}')
      add_edge(guid_in_domain(i, domain), op_start, 0)
  for o in outputs:
    if o:
#      print (f'OPO: [{o}] LINE: {line}')
      add_edge(op_end, guid_in_domain(o, domain), 0)
  

def rel(line):
  ms, _, rest = extract_common(line)
  m = regexp_rel.match(rest)
  guid = m.group(1)
#  print (f'REL: [{guid}] LINE: {line}')
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


VERTICES=set()
SOURCES=set()
TARGETS=set()
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

DISTANCE = {}
PRED = {}
for v in VERTICES:
  DISTANCE[v] = 10000000000000000
  PRED[v] = None

DISTANCE['START'] = 0
DISTANCE['END'] = 10000000000000000

EDGES=[]
for v in GRAPH:
  for l in GRAPH[v]:
    vv = l[0]
    w = l[1]
    e = (v,vv,-w)
    EDGES.append(e)

for e in EDGES:
  print (e)

for _ in range(len(VERTICES)):
  for e in EDGES:
    src = e[0]
    dst = e[1]
    w = e[2]
    if DISTANCE[src] + w < DISTANCE[dst]:
      DISTANCE[dst] = DISTANCE[src] + w
      PRED[dst] = src


v = 'END'
while PRED[v] != 'START':
  print (PRED[v])
  v = PRED[v]

  



