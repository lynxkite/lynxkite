#!/usr/bin/env python
'''
Reads a json representation of all the running clusters from stdin and then prints the id
that corresponds to the name of the cluster (the name is the only command line
argument)
'''
import json
import sys

cluster_info = sys.stdin.read()
name = sys.argv[1]

clusters = json.loads(cluster_info)['Clusters']

my_clusters = list(filter(lambda cluster: cluster['Name'] == name, clusters))


if not my_clusters:
  sys.stderr.write('Could not find a cluster called "' + name + '"\n')
  sys.exit(1)

if len(my_clusters) > 1:
  msg = 'Name: "' + name + '" occurs in several clusters:\n' + \
      '\n'.join(' -> ' + str(c) for c in my_clusters)
  sys.stderr.write(msg + '\n')
  sys.exit(1)

print (my_clusters[0]['Id'])
