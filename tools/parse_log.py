#!/usr/bin/env python
import argparse
import string
import fileinput
import json
from sets import Set


flags = argparse.ArgumentParser(
  description='Parses a LynxKite log')

flags.add_argument('--logfile', help='The name of the logfile', required=True)
flags.add_argument('--just_check', help='Just check json in the logfile', dest='just_check', action='store_true')
flags.set_defaults(just_check=False)

flags = flags.parse_args()

all_jsons=[]

# Operation logger
operation_logger_marker = 'OPERATION_LOGGER_MARKER'

def check_json(js):
    assert(Set(js.keys()) ==  Set(['name', 'guid', 'elapsedMs', 'inputs', 'outputs']))
    for o in js['outputs']:
        assert(Set(o.keys()) == Set(['name', 'gUID', 'partitions', 'count']))    
    for i in js['inputs']:
        assert(Set(i.keys()) == Set(['name', 'gUID', 'partitions', 'count']) or
               Set(i.keys()) == Set(['name', 'gUID', 'partitions']))

def parse_operation_logs(line):
    js = json.loads(line)
    check_json(js)
    if flags.just_check == True:
        return
    all_jsons.append(js)
    # name = js['name']
    # guid = js['guid']
    # elapsedMs = js['elapsedMs']
    # inputs = js['inputs']
    # outputs = js['outputs']
    # print "*****************"
    # print type(name), name
    # print type(guid), guid
    # print type(elapsedMs), elapsedMs
    # print 'outputs'
    # for o in outputs:
    #     print '      ' + str(o)
    # print 'inputs'    
    # for i in inputs:
    #     print '      ' + str(i)
    


processors = {
    operation_logger_marker: parse_operation_logs
}


def get_handler(line):
    for marker in processors.keys():
        if marker in line:
            return (marker, processors.get(marker))
    return None

def get_relevant_part_of_the_line(marker, line):
    idx = line.find(marker)
    assert(idx != -1)
    stuff_begins = idx + len(marker)
    return line[stuff_begins:]
    

def process_file(filename):
    with open(filename) as file:
        for line in file:
            h = get_handler(line)
            if h is not None:
                h[1](get_relevant_part_of_the_line(h[0], line))

def main():            
    process_file(flags.logfile)
    print json.dumps(all_jsons, indent=8)


main()
