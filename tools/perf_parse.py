#!/usr/bin/env python3
'''
Parses a LynxKite log file (from stdin) and dumps a performance measurement summary based
on the loglines that contain either the word OPERATION_LOGGER_MARKER or RELOCATION_LOGGER_MARKER
'''

import fileinput
import re
from collections import defaultdict
from prettytable import PrettyTable


OPS = defaultdict(list)
RELS = defaultdict(list)
INPUTS = defaultdict(set)
RELOCATION_TIMES = defaultdict(list)

regexp_common = re.compile(
    r'^.*elapsed: (\d+) (OPERATION_LOGGER_MARKER|RELOCATION_LOGGER_MARKER) (.*)')
regexp_rel = re.compile('Moving ([^ ]+) from ([^ ]+) to ([^ ]+)')
regexp_op = re.compile('([^ ]+) opguid: ([^ ]+) inputs: ([^ ]+) op: (.*)')


def extract_common(line):
  m = regexp_common.match(line)
  return int(m.group(1)), m.group(2), m.group(3)


def op(line):
  ms, _, rest = extract_common(line)
  m = regexp_op.match(rest)
  domain = m.group(1)
  opguid = m.group(2)
  inputs = m.group(3)
  inputs = inputs[1:len(inputs) - 2].split(',')
  op = m.group(4)
  if 'com.lynxanalytics.biggraph.graph_operations.ImportDataFrame' in op:
    op = 'ImportDataFrame()'
  idx = op.find('(')
  op = op[:idx]
  op = f'{op} [{domain}]'
  for i in inputs:
    if i:
      INPUTS[op].add(i)
  OPS[op].append(ms)


def rel(line):
  ms, _, rest = extract_common(line)
  m = regexp_rel.match(rest)
  guid = m.group(1)
  src = m.group(2)
  dst = m.group(3)
  RELOCATION_TIMES[guid].append(ms)
  RELS[f'{src}->{dst}'].append(ms)


for line in fileinput.input():
  line = line.rstrip()
  if 'OPERATION_LOGGER_MARKER' in line:
    op(line)
  elif 'RELOCATION_LOGGER_MARKER' in line:
    rel(line)


def print_table(title, field_names, diclist):
  print(title)
  t = PrettyTable()
  t.field_names = field_names
  for i in field_names:
    t.align[i] = 'l'

  for n in diclist:
    v = diclist[n]
    s = sum(v)
    count = len(v)
    avg = '{0:.0f}'.format(s / count)
    if title == 'ALL OPERATIONS':
      rsum = 0
      rnum = 0
      for g in INPUTS[n]:
        rt = RELOCATION_TIMES[g]
        rsum = rsum + sum(rt)
        if len(rt) > 0:
          rnum = rnum + 1
      t.add_row([n, s, count, avg, rsum, rnum, len(INPUTS[n])])
    else:
      t.add_row([n, s, count, avg])
  t.sortby = 'Sum (ms)'
  t.reversesort = True
  print(t)


print_table('ALL OPERATIONS',
            ['Operation', 'Sum (ms)', 'Count', 'Avg', 'input relocation time',
             'relocated inputs', 'all inputs'],
            OPS)
print()
print_table('ALL_RELOCATIONS', ['Relocation', 'Sum (ms)', 'Count', 'Avg'], RELS)
