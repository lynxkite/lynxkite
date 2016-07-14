import argparse
import random
import sys


flags = argparse.ArgumentParser(description='Generates random edges.')
flags.add_argument('--vertices', type=int, help='Vertex count.', required=True)
flags.add_argument(
    '--degree',
    type=float,
    help='Average out degree.',
    default=10)
flags.add_argument(
    '--edge_file',
    type=str,
    help='Write edges to this file.',
    required=True)
flags.add_argument('--loop_chance', type=float,
                   help='Average number of loops per vertex.', default=0.0)


def main(args):
  with file(args.edge_file, 'w') as f:
    f.write('src,dst,x\n')
    for i in range(int(args.degree * args.vertices)):
      a = b = random.randint(1, args.vertices)
      while a == b:
        b = random.randint(1, args.vertices)
      x = random.random()
      f.write('{},{},{}\n'.format(a, b, x))
    # Add loops.
    for i in range(int(args.loop_chance * args.vertices)):
      a = random.randint(1, args.vertices)
      x = random.random()
      f.write('{},{},{}\n'.format(a, a, x))


if __name__ == '__main__':
  main(flags.parse_args())
