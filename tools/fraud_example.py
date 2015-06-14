"""Generates a graph of a mobile app store entities: developers, apps, credit cards, etc.

A fraction of the entities have been reported as fraudulent. The components with reported
entities can be all considered suspect and banned/verified by the operator.
"""

import random
import sys

def rnd():
  return random.random()

def geomchoice(s):
  options = s.split()
  n = len(options)
  for i in range(n):
    for j in range(i, n):
      options.append(options[i])
  return random.choice(options)

def choice(s):
  options = s.split()
  return random.choice(options)

class Node(object):
  def __init__(self, ID):
    self.ID = ID
    self.suspect = 0
    self.reported = 0

  def __str__(self):
    return '{ID},{kind},{label},{icon},{reported},{suspect}'.format(**self.__dict__)

class Edge(object):
  def __init__(self, a, b):
    self.src = a.ID
    self.dst = b.ID

  def __str__(self):
    return '{src},{dst}'.format(**self.__dict__)

class RandomGraph(object):
  def __init__(self, n):
    self.nodes = []
    self.edges = []
    self.developers = []
    self.suspects = []
    for i in range(n / 5):
      self.addDeveloper()
    for i in range(n / 5, n):
      self.addNode()

  def hookup(self, n, dist):
    degree = int(choice(dist))
    if rnd() < 0.05:
      n.suspect = 1
      if n.kind != 'app' and rnd() < 0.2:
        degree += 1

    for i in range(degree):
      if n.suspect:
        dev = random.choice(self.suspects)
      else:
        dev = random.choice(self.developers)
      self.edges.append(Edge(dev, n))

  def addDeveloper(self):
    n = Node(len(self.nodes))
    n.kind = 'developer'
    n.icon = 'person'
    n.label = '*' * random.randrange(3, 5) + '@' + geomchoice('gmail.com yahoo.com hotmail.com mail.ru')
    self.developers.append(n)
    if n.label.endswith('.ru') and rnd() < 0.1:
      n.suspect = 1
      self.suspects.append(n)
    elif rnd() < 0.01:
      n.suspect = 1
      self.suspects.append(n)
    self.nodes.append(n)

  def addNode(self):
    n = Node(len(self.nodes))
    if rnd() < 0.1:
      n.kind = 'credit card'
      n.icon = 'sim'
      n.label = '**** **** **** ****'
      self.hookup(n, '1')
    elif rnd() < 0.1:
      n.kind = 'phone number'
      n.icon = 'phone'
      n.label = '+1 (***) ***-****'
      self.hookup(n, '1')
    elif rnd() < 0.1:
      n.kind = 'IP address'
      n.icon = 'home'
      n.label = '***.***.***.' + str(random.randrange(10, 250))
      self.hookup(n, '1')
    else:
      n.kind = 'app'
      n.icon = 'triangle'
      if rnd() < 0.5:
        n.label = choice('Clash Saga Chronicles War Game') + ' of ' + choice('Empires Clans Witches Dragons Mutants Zombies Pirates')
      else:
        n.label = choice('Free Magic Super Funny Hot') + ' ' + choice('Cats Ladies Dogs Games Drawing Tunes') + ' ' + choice('Pro Free 2 3 Extreme Professional 4 5 6')
      self.hookup(n, '1')
    if n.suspect and rnd() < 0.05:
      n.reported = 1
    self.nodes.append(n)

def nodeHeader():
  header = Node('ID')
  header.kind = 'kind'
  header.label = 'label'
  header.icon = 'icon'
  header.reported = 'reported'
  header.suspect = 'suspect'
  return header

def edgeHeader():
  header = Edge(Node('src'), Node('dst'))
  return header

def main():
  n = int(sys.argv[1])
  g = RandomGraph(n)
  with file('nodes.csv', 'w') as f:
    f.write(str(nodeHeader()) + '\n')
    for node in g.nodes:
      f.write(str(node) + '\n')
  with file('edges.csv', 'w') as f:
    f.write(str(edgeHeader()) + '\n')
    for edge in g.edges:
      f.write(str(edge) + '\n')

if __name__ == '__main__':
  main()
