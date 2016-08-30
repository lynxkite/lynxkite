#!/usr/bin/python

'''Outputs a matrix with minimal intersection sizes for cliques to be in the same community'''

import math

adjacencyThreshold = 0.6

print '%f,%s' % (adjacencyThreshold, ','.join([str(v) for v in range(1, 11)]))
for a in range(1, 11):
  values = [int(math.ceil(adjacencyThreshold * (a + b) * (a * a + b * b) / (4 * a * b)))
            for b in range(1, 11)]
  print '%d,%s' % (a, ','.join([str(v) for v in values]))
