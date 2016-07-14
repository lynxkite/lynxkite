"""Test data generator for a cell phone subscriber database.

Run without arguments to generate two CSV files. Run with --help for more options.
"""
import argparse
import collections
import math
import random
import sys

random.seed(0)
parser = argparse.ArgumentParser()
parser.add_argument(
    '--nodes',
    help='Number of nodes in the graph.',
    type=int,
    default=1000)
parser.add_argument(
    '--tree',
    help='Generate a tree instead of a random graph.',
    action='store_true')

Person = collections.namedtuple(
    'Person', 'id, age, income, subscription, gender')


def rnd():
  """A normal-ish distribution between 0.0 and 1.0."""
  return (random.random() + random.random() + random.random() +
          random.random() + random.random()) / 5


def vertices(n):
  vs = []
  for i in range(n):
    if random.random() < 0.5:
      gender = 'male'
      age_max = 120
    else:
      gender = 'female'
      age_max = 140
    age = rnd() * rnd() * age_max
    income = 100000 * rnd()
    if gender == 'male':
      income *= 0.2
    income *= 20 / (abs(age - 40) + 20)
    if random.random() < 0.1:
      subscription = 'Talk Mania'
    else:
      subscription = 'Basic'
    if income > rnd() * 100000:
      subscription = 'Executive Plus'
    if random.random() < 0.2:
      if gender == 'male' or random.random() < 0.2:
        subscription = 'Extreme Hazard Pro'
    if random.random() < 0.2:
      if gender == 'female' or random.random() < 0.2:
        subscription = 'Family Plus'
    if age < rnd() * 20:
      if random.random() < 0.1:
        subscription = 'Little Devil'
      else:
        subscription = 'Little Genius'
    vs.append(Person(i, age, income, subscription, gender))
  return vs


def treeedges(vs):
  es = []
  for dst in range(1, len(vs)):
    src = random.randrange(dst)
    es.append((src, dst))
  return es


def edges(vs):
  es = []
  degree = {}
  degrees = 1
  for i in range(len(vs)):
    degree[i] = 1
    for j in range(i):
      p = 0.5 * degree[j] / degrees
      if vs[j].gender == 'female':
        p *= 5
      if vs[j].subscription == 'Talk Mania':
        p *= 5
      if random.random() < p:
        es.append((i, j))
        es.append((j, i))
        degree[i] += 1
        degree[j] += 1
        degrees += 1
  return es


def main():
  args = parser.parse_args()
  vs = vertices(args.nodes)
  if args.tree:
    es = treeedges(vs)
  else:
    es = edges(vs)

  with file('subscriber-demo-{}-vertices.csv'.format(args.nodes), 'w') as f:
    for v in vs:
      f.write(','.join(str(x) for x in v) + '\n')

  with file('subscriber-demo-{}-edges.csv'.format(args.nodes), 'w') as f:
    for e in es:
      f.write(','.join(str(x) for x in e) + '\n')

if __name__ == '__main__':
  main()
