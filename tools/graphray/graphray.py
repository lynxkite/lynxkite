#!/usr/bin/env python3
'''Renders graphs with POV-Ray.'''
import networkx as nx
import os
import pandas as pd
import PIL.Image
import PIL.ImageChops
import PIL.ImageOps
import subprocess


def povray(output_file, graph, width, height, shadow_pass):
  vs, es = graph
  vertices = '\n'.join(
      f'sphere {{ <{v.x}, {v.y}, {v.r}>, {v.r} Color(rgb <{v.color}>) }}'
      for v in vs.to_records())

  def edge(src, dst):
    return f'''
cylinder {{ <{src.x}, {src.y}, {src.r}>, <{dst.x}, {dst.y}, {dst.r}>, {0.05 * (src.r + dst.r)} }}
    '''.strip()
  edges = '\n'.join(edge(vs.loc[e.src], vs.loc[e.dst]) for e in es.to_records())
  with open('tmp.pov', 'w') as f:
    f.write(f'''
#version 3.7;
#include "{os.path.dirname(__file__)}/scene.pov"

Center_Object(
  union {{
    {vertices}
    {edges}
    Color(rgb 1.0)
  }}
  , x + y)
''')
  p = subprocess.run([
      'povray',
      '+A0.05',  # Anti-aliasing.
      f'+W{width}',  # Width.
      f'+H{height}',  # Height.
      '+UA',  # Output alpha.
      '-D',  # No display.
      '+Itmp.pov',
      '+O' + output_file,
      'Declare=shadow_pass=' + str(shadow_pass),
  ], stderr=subprocess.PIPE)
  if p.returncode:
    print(p.stderr.decode('utf8'))
  p.check_returncode()
  os.remove('tmp.pov')


def layout(graph):
  vs, es = graph
  graph = nx.Graph()
  graph.add_edges_from(zip(es.src, es.dst))
  pos = nx.kamada_kawai_layout(graph)
  vs['x'] = [pos[v][0] for v in vs.index]
  vs['y'] = [pos[v][1] for v in vs.index]


def compose(output_file, graph, width, height):
  layout(graph)
  povray('obj.png', graph, width, height, shadow_pass=0)
  povray('shadow.png', graph, width, height, shadow_pass=1)
  obj = PIL.Image.open('obj.png')
  shadow = PIL.Image.open('shadow.png').convert('L')
  # Make shadow render a bit brighter so that unshadowed parts are perfectly white.
  shadow = shadow.point(lambda x: 1.1 * x)
  # Turn grayscale shadow into full black with alpha.
  unshadow = PIL.ImageChops.invert(shadow)
  black = unshadow.copy()
  black.paste(0, (0, 0) + black.size)
  shadow = PIL.Image.merge('RGBA', (black, black, black, unshadow))
  # Composite alpha shadow under the object.
  PIL.Image.alpha_composite(shadow, obj).save(output_file, 'png')
  os.remove('obj.png')
  os.remove('shadow.png')


def demo_graph():
  vs = pd.DataFrame(dict(
      color=['1, 0, 0', '0, 1, 0', '1, 1, 1'],
      r=[0.1, 0.2, 0.3],
  ))
  es = pd.DataFrame(dict(
      src=[0, 1, 2],
      dst=[1, 2, 0],
  ))
  return vs, es


def render(vs, es, width=600, height=600):
  '''Renders a graph and displays it in the notebook.'''
  compose('graph.png', (vs, es), width, height)
  from IPython.display import Image
  return Image(filename='graph.png')


def main():
  compose('graph.png', demo_graph())


if __name__ == '__main__':
  main()
