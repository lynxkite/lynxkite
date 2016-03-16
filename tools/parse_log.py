#!/usr/bin/env python
import argparse
import string
import fileinput
import json
from sets import Set


flags = argparse.ArgumentParser(description='Parses a LynxKite log')

flags.add_argument('--logfile', help='The name of the logfile', required=True)

flags = flags.parse_args()

all_jsons=[]

# Operation logger
operation_logger_marker = 'OPERATION_LOGGER_MARKER'

def parse_operation_logs(line):
  js = json.loads(line)
  all_jsons.append(js)


processors = {
  operation_logger_marker: parse_operation_logs
}


def get_handler(line):
  for marker in processors:
    if marker in line:
      return (marker, processors[marker])
  return None

def get_relevant_part_of_the_line(marker, line):
  idx = line.find(marker)
  assert(idx != -1)
  stuff_begins = idx + len(marker)
  return line[stuff_begins:]


def process_file(filename):
  with open(filename) as f:
    for line in f:
      h = get_handler(line)
      if h is not None:
        h[1](get_relevant_part_of_the_line(h[0], line))

def main():
  process_file(flags.logfile)
  print json.dumps(all_jsons, indent=8)

main()
