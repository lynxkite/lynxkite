import argparse
import random
import string
import sys


flags = argparse.ArgumentParser(
    description='Generates test data for fingerprinting.')
flags.add_argument('--vertices', type=int, help='Vertex count.', required=True)
flags.add_argument(
    '--degree',
    type=float,
    help='Average out degree.',
    default=10)
flags.add_argument(
    '--matched',
    type=float,
    help='Ratio of vertices already matched.',
    default=0.9)
flags.add_argument(
    '--symmetric',
    type=float,
    help='Ratio of symmetric edges.',
    default=0.5)
flags.add_argument(
    '--noise',
    type=float,
    help='Noise to add after splitting.',
    default=0.1)
flags.add_argument(
    '--edge_file',
    type=str,
    help='Write edges to this file.',
    required=True)
flags.add_argument(
    '--vertex_file',
    type=str,
    help='Write vertices to this file.',
    required=True)


def rndstr(n):
  return ''.join(random.sample(string.lowercase, n))


class Vertex(object):

  def __init__(self, index):
    self.index = index
    self.email = rndstr(5) + '@' + rndstr(3) + '.com'
    self.name = rndstr(3).capitalize() + ' ' + rndstr(5).capitalize()
    self.in_neighbors = []
    self.out_neighbors = []


def main(args):
  # Generate graph.
  vertices = []
  edges_per_vertex = args.degree / (1.0 + args.symmetric)
  dither = 0
  for i in range(args.vertices):
    v = Vertex(i)
    vertices.append(v)
    edges_back = edges_per_vertex * 2 * i / (args.vertices - 1) + dither
    dither = edges_back - int(edges_back)
    for j in range(int(edges_back)):
      w = random.randint(0, i - 1)
      sym = random.random() < args.symmetric
      out = random.random() < 0.5
      if sym or out:
        vertices[w].in_neighbors.append(i)
        v.out_neighbors.append(w)
      if sym or not out:
        v.in_neighbors.append(w)
        vertices[w].out_neighbors.append(i)
  # Split some vertices.
  to_split = random.sample(vertices, int(args.vertices * (1.0 - args.matched)))
  for v in to_split:
    v.orig_out_neighbors = v.out_neighbors[:]
    v.orig_in_neighbors = v.in_neighbors[:]
  for v in to_split:
    w = Vertex(len(vertices))
    vertices.append(w)
    w.name = v.name
    w.email = ''
    v.name = ''
    for n in v.orig_out_neighbors:
      if random.random() < args.noise and n in v.out_neighbors:
        v.out_neighbors.remove(n)
        vertices[n].in_neighbors.remove(v.index)
      if random.random() > args.noise:
        w.out_neighbors.append(n)
        vertices[n].in_neighbors.append(w.index)
    for n in v.orig_in_neighbors:
      if random.random() < args.noise and n in v.in_neighbors:
        v.in_neighbors.remove(n)
        vertices[n].out_neighbors.remove(v.index)
      if random.random() > args.noise:
        w.in_neighbors.append(n)
        vertices[n].out_neighbors.append(w.index)
  # Export graph.
  with file(args.edge_file, 'w') as f:
    f.write('src,dst\n')
    for v in vertices:
      for w in v.out_neighbors:
        f.write('{},{}\n'.format(v.index, w))
  with file(args.vertex_file, 'w') as f:
    f.write('id,email,name\n')
    for v in vertices:
      f.write('{},{},{}\n'.format(v.index, v.email, v.name))


if __name__ == '__main__':
  main(flags.parse_args())
