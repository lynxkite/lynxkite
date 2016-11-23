"""Generates a lattice network of the given size. Vertex attributes are generated for a machine
learning exercise.

Run without arguments to generate vertices.csv and edges.csv. Run with --help for more options.
"""
import argparse
import random

random.seed(0)
parser = argparse.ArgumentParser()
parser.add_argument(
  '--vertices_file', help='Name of vertex CSV file.', type=str, default='vertices.csv')
parser.add_argument(
  '--edges_file', help='Name of edge CSV file.', type=str, default='edges.csv')
parser.add_argument(
  '--width', help='Lattice width.', type=int, default=1000)
parser.add_argument(
  '--height', help='Lattice height.', type=int, default=1000)
parser.add_argument(
  '--left_percent', help='Percent of vertices to mark as "left".', type=int, default=40)
parser.add_argument(
  '--unknown_percent', help='Percent of vertices with "missing" data.', type=int, default=20)

rnd = random.randrange

def main():
  args = parser.parse_args()
  W = args.width
  H = args.height
  def vid(x, y):
    return str(x * W + y)

  with file(args.edges_file, 'w') as f:
    def edge(a, b):
      f.write(a + ',' + b + '\n')
    f.write('src,dst\n')
    for x in range(W):
      for y in range(H):
        if x != W - 1:
          edge(vid(x, y), vid(x + 1, y))
        if y != H - 1:
          edge(vid(x, y), vid(x, y + 1))

  with file(args.vertices_file, 'w') as f:
    def vertex(id, side):
      unknown = random.random() * 100 < args.unknown_percent
      f.write(id + ',' + side + ',' + ('' if unknown else side) + '\n')
    f.write('id,side_truth,side\n')
    for x in range(W):
      for y in range(H):
        if x < W * args.left_percent / 100:
          vertex(vid(x, y), 'left')
        else:
          vertex(vid(x, y), 'right')


if __name__ == '__main__':
  main()
