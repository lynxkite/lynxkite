#!/usr/bin/env python

"""Given two name lists finds pairs (w1 \in list1, w2 \in list) such that w1 and w2 are similar."""

import sys


def edit_distance(s1, s2):
  m = len(s1) + 1
  n = len(s2) + 1

  tbl = {}
  for i in range(m):
    tbl[i, 0] = i
  for j in range(n):
    tbl[0, j] = j
  for i in range(1, m):
    for j in range(1, n):
      cost = 0 if s1[i - 1] == s2[j - 1] else 1
      tbl[
          i,
          j] = min(
          tbl[
              i,
              j - 1] + 1,
          tbl[
              i - 1,
              j] + 1,
          tbl[
              i - 1,
              j - 1] + cost)

  return tbl[i, j]


def sorted_name(n):
  parts = sorted(n.split(' '))
  return ' '.join(parts)


def are_similar(n1, n2):
  sn1 = sorted_name(n1)
  sn2 = sorted_name(n2)
  return edit_distance(sn1, sn2) < 4


def read_list(fn):
  return [line.strip() for line in open(fn).readlines()]

list1 = read_list(sys.argv[1])
list2 = read_list(sys.argv[2])

i = 0
l = len(list1)
for n1 in list1:
  for n2 in list2:
    if are_similar(n1, n2):
      print "%s,%s" % (n1, n2)
  i += 1
  sys.stderr.write("Processed %2.2f%%\n" % (i * 100.0 / l))
