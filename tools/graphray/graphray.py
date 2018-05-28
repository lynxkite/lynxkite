#!/usr/bin/env python3
'''
Renders graphs with POV-Ray. Can be used either in a notebook through the render() function
or from the command line by piping in JSON and piping out PNG.
'''
import networkx as nx
import os
import pandas as pd
import PIL.Image
import PIL.ImageChops
import PIL.ImageOps
import subprocess
import tempfile


def povray(output_file, graph, width, height, quality, shadow_pass):
  vs, es = graph
  vs = vs.fillna(0)
  if 'shape' not in vs:
    vs['shape'] = 'sphere'
  if 'color' not in vs:
    vs['color'] = '1,1,1'
  if 'highlight' not in vs:
    vs['highlight'] = 0
  if 'r' not in vs:
    vs['r'] = 0.1

  def vertex(v, ignore_highlight=False):
    if v['highlight'] and not ignore_highlight:
      return f'''
union {{
  object {{ disc {{ <{v.x}, {v.y}, 0>, z, {v.r * 3}, {v.r * 2} }} Color(rgb(<1,0.5,0.5>)) }}
  {vertex(v, True)}
}}'''
    elif v['shape'] == 'guy':
      return f'''
union {{
  sphere {{ <{v.x}, {v.y}, {v.r * 2}>, {v.r} Color(rgb <{v.color}>) }}
  Round_Cone(<{v.x}, {v.y}, 0>, {v.r}, <{v.x}, {v.y}, {v.r * 2}>, {v.r * 0.5}, {v.r * 0.1}, 0)
  Color(rgb <{v.color}>)
}}'''
    elif v['shape'] == 'cube':
      return f'''
object {{
  Round_Box(<{v.x - v.r}, {v.y - v.r}, 0>, <{v.x + v.r}, {v.y + v.r}, {v.r * 2}>, {v.r * 0.1}, 0)
  Color(rgb <{v.color}>)
}}
'''
    elif v['shape'] == 'cylinder':
      return f'''
object {{
  Round_Cylinder(<{v.x}, {v.y}, 0>, <{v.x}, {v.y}, {v.r * 2}>, {v.r}, {v.r * 0.1}, 0)
  Color(rgb <{v.color}>)
}}
'''
    else:
      return f'sphere {{ <{v.x}, {v.y}, {v.r}>, {v.r} Color(rgb <{v.color}>) }}'

  def edge(src, dst):
    return f'''
cylinder {{ <{src.x}, {src.y}, {src.r}>, <{dst.x}, {dst.y}, {dst.r}>, {0.05 * (src.r + dst.r)} }}
    '''.strip()

  vertices = '\n'.join(vertex(v) for v in vs.to_records())
  edges = '\n'.join(edge(vs.loc[e.src], vs.loc[e.dst]) for e in es.to_records() if e.src != e.dst)
  with open('tmp.pov', 'w') as f:
    f.write(f'''
#version 3.7;
#include "{os.path.dirname(__file__) or '.'}/scene.pov"

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
      f'+Q{quality}',
      f'+W{width}',
      f'+H{height}',
      '+UA',  # Output alpha.
      '-D',  # No display.
      '+Itmp.pov',
      '+O' + output_file,
      'Declare=shadow_pass=' + str(shadow_pass),
  ], stderr=subprocess.PIPE)
  if p.returncode:
    print(p.stderr.decode('utf8'))
  p.check_returncode()


def layout(graph):
  vs, es = graph
  if 'x' not in vs:
    graph = nx.Graph()
    graph.add_edges_from(zip(es.src, es.dst))
    pos = nx.kamada_kawai_layout(graph)
    vs['x'] = [pos[v][0] for v in vs.index]
    vs['y'] = [pos[v][1] for v in vs.index]


def compose(graph, width, height, quality):
  layout(graph)
  with tempfile.TemporaryDirectory() as d:
    os.chdir(d)
    povray('obj.png', graph, width, height, quality, shadow_pass=0)
    povray('shadow.png', graph, width, height, quality, shadow_pass=1)
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
    PIL.Image.alpha_composite(shadow, obj).save('graph.png', 'png')
    with open('graph.png', 'rb') as f:
      return f.read()


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


def render(vs, es, width=1600, height=1000, quality=9):
  '''Renders a graph and displays it in the notebook.'''
  data = compose((vs, es), width, height, quality)
  from IPython.display import Image
  return Image(data=data)


def main():
  import json
  import sys
  config = json.load(sys.stdin)
  graph = pd.DataFrame(config['vs']), pd.DataFrame(config['es'])
  data = compose(graph, config['width'], config['height'], config['quality'])
  sys.stdout.buffer.write(data)


if __name__ == '__main__':
  main()
