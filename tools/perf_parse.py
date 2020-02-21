#!/usr/bin/env python3
'''
Parses a LynxKite log file (from stdin) and dumps a performance measurement summary based
on the loglines that contain either the word OPERATION_LOGGER_MARKER or RELOCATION_LOGGER_MARKER
'''

import fileinput
import re
from collections import defaultdict
from prettytable import PrettyTable


BY_PHASE_OP = defaultdict(lambda: defaultdict(list))
TOTAL_OP = defaultdict(list)
BY_PHASE_REL = defaultdict(lambda: defaultdict(list))
TOTAL_REL = defaultdict(list)

regexp_common = re.compile(
    r'^.*(phase\d+) elapsed: (\d+) (OPERATION_LOGGER_MARKER|RELOCATION_LOGGER_MARKER) (.*)')
regexp_rel = re.compile('Moving ([^ ]+) from ([^ ]+) to ([^ ]+)')
regexp_op = re.compile('([^ ]+) opguid: ([^ ]+) inputs: ([^ ]+) op: (.*)')


def extract_common(line):
  m = regexp_common.match(line)
  return m.group(1), int(m.group(2)), m.group(3), m.group(4)


def op(line):
  phase, ms, _, rest = extract_common(line)
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
  BY_PHASE_OP[phase][op].append(ms)
  TOTAL_OP[op].append(ms)


def rel(line):
  phase, ms, _, rest = extract_common(line)
  m = regexp_rel.match(rest)
  guid = m.group(1)
  src = m.group(2)
  dst = m.group(3)
  BY_PHASE_REL[phase][f'{src}->{dst}'].append(ms)
  TOTAL_REL[f'{src}->{dst}'].append(ms)


for line in fileinput.input():
  line = line.rstrip()
  if 'OPERATION_LOGGER_MARKER' in line:
    op(line)
  elif 'RELOCATION_LOGGER_MARKER' in line:
    rel(line)


def print_table(title, name, diclist):
  print(title)
  t = PrettyTable()
  field_names = [name, 'Sum (ms)', 'Count', 'Avg']
  t.field_names = field_names
  for i in field_names:
    t.align[i] = 'l'

  for n in diclist:
    v = diclist[n]
    s = sum(v)
    count = len(v)
    avg = '{0:.0f}'.format(s / count)
    t.add_row([n, s, count, avg])
  t.sortby = 'Sum (ms)'
  t.reversesort = True
  print(t)


print_table('ALL OPERATIONS', 'Operation', TOTAL_OP)
print()
print_table('ALL_RELOCATIONS', 'Relocation', TOTAL_REL)
print()
if len(BY_PHASE_OP) > 1:
  for p in BY_PHASE_OP:
    print_table(f'PHASE: {p}', 'Operation', BY_PHASE_OP[p])
    print()
if len(BY_PHASE_REL) > 1:
  for p in BY_PHASE_REL:
    print_table(f'PHASE: {p}', 'Relocation', BY_PHASE_REL[p])
    print()
