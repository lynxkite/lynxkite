'''Creates an animated SVG based on an SVG that approximates each curve of the logo by two ellipses.

1. Edit progress-editable.svg.
2. Run this script. (Ignore some errors if this is the first run.)
3. Open progress-before-cut.svg.
4. Cut the transformed circles from the base circles.
5. Save as progress-after-cut.svg.
6. Run the script again.
'''
import numpy as np
from lxml import etree


def matrix(a, b, c, d, e, f):
  return np.array([[a, c, e], [b, d, f], [0, 0, 1]])


def tostr(m):
  return f'matrix({m[0, 0]}, {m[1, 0]}, {m[0, 1]}, {m[1, 1]}, {m[0, 2]}, {m[1, 2]})'


SVG = '{http://www.w3.org/2000/svg}'
ms = []
tree = etree.parse('progress-editable.svg')
for n in tree.getroot().iter():
  if 'letter' in n.get('id', ''):
    n.getparent().remove(n)
  if n.tag == SVG + 'ellipse':
    m = eval(n.get('transform'))
    cx, cy, rx, ry = [float(n.get(f)) for f in 'cx cy rx ry'.split()]
    t = m @ np.array([cx, cy, 1])
    m = m @ np.array([[1, 0, 0], [0, ry / rx, 0], [0, 0, 1]])
    m[:, 2] = t
    if n.get('id').startswith('disc'):
      ms.append(m)
      n.attrib.pop('transform')
    else:
      n.set('transform', tostr(np.linalg.inv(ms[-1]) @ m))
    n.set('cx', str(len(ms) * 100))
    n.set('cy', '0')
    n.set('r', str(rx))
    n.attrib.pop('rx')
    n.attrib.pop('ry')
    n.tag = 'circle'

with open('progress-before-cut.svg', 'wb') as f:
  f.write(etree.tostring(tree, pretty_print=True))

durs = ['2.5s', '3s', '2.2s', '2s']
colors = ['00273e', '39bcf3', '39bcf3', '39bcf3']
tree = etree.parse('progress-after-cut.svg')
for n in list(tree.getroot().iter()):
  if 'disc' in n.get('id', ''):
    m = ms.pop(0)
    shift = 100 * (4 - len(ms))
    m = m @ matrix(1, 0, 0, 1, -shift, 0)
    n.set('style', 'fill:#' + colors.pop(0))
    g = etree.SubElement(n.getparent(), 'g')
    n.getparent().remove(n)
    g.set('transform', tostr(m))
    g.append(n)
    anim = etree.SubElement(n, 'animateTransform')
    anim.set('attributeType', 'xml')
    anim.set('attributeName', 'transform')
    anim.set('type', 'rotate')
    anim.set('from', f'0 {shift} 0')
    anim.set('to', f'-360 {shift} 0')
    anim.set('dur', durs.pop(0))
    anim.set('keySplines', '0.5 0.1 0.5 0.9')
    anim.set('keyTimes', '0;1')
    anim.set('calcMode', 'spline')
    anim.set('repeatCount', 'indefinite')
with open('progress.svg', 'wb') as f:
  f.write(etree.tostring(tree, pretty_print=True))
