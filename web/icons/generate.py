#!/usr/bin/env python3
'''Generates icons for all boxes.'''
import os
import PIL.Image
import PIL.ImageChops
import PIL.ImageOps
import subprocess
import unicodedata


def povray(output_file, font, caption, shadow_pass):
  with open('tmp.pov', 'w') as f:
    f.write('''
#version 3.7;
#include "scene.pov"
Statue("{}", "{}")
'''.format(font, caption))
  subprocess.run([
      'povray',
      '+A0.05',  # Anti-aliasing.
      '+W200',  # Width.
      '+H200',  # Height.
      '+UA',  # Output alpha.
      '-D',  # No display.
      '+Itmp.pov',
      '+O' + output_file,
      'Declare=shadow_pass=' + str(shadow_pass),
  ]).check_returncode()
  os.remove('tmp.pov')


def compose(output_file, font, caption):
  output_file = '../app/images/icons/' + output_file
  if os.path.exists(output_file):
    return
  povray('obj.png', font, caption, shadow_pass=0)
  povray('shadow.png', font, caption, shadow_pass=1)
  obj = PIL.Image.open('obj.png')
  shadow = PIL.Image.open('shadow.png').convert('L')
  # Make shadow render a bit brighter so that unshadowed parts are perfectly white.
  shadow = shadow.point(lambda x: 1.1 * x)
  # Turn grayscale shadow into full black with alpha.
  unshadow = PIL.ImageChops.invert(shadow)
  black = unshadow.copy()
  black.paste(0)
  shadow = PIL.Image.merge('RGBA', (black, black, black, unshadow))
  # Composite alpha shadow under the object.
  PIL.Image.alpha_composite(shadow, obj).save(output_file, 'png')
  os.remove('obj.png')
  os.remove('shadow.png')


def lookup_material_codepoint(name, registry={}):
  if not registry:
    for line in open('codepoints'):
      line = line.strip()
      if line:
        k, v = line.split()
        registry[k] = chr(int(v, 16))
  return registry[name]


def render(name):
  try:
    font = 'NotoSansSymbols-Regular.ttf'
    character = unicodedata.lookup(name.upper())
  except KeyError:  # Not a Unicode character name.
    font = 'MaterialIcons-Regular.ttf'
    character = lookup_material_codepoint(name.lower())
  compose(name.replace(' ', '_') + '.png', font, character)


def main():
  if os.path.dirname(__file__):
    os.chdir(os.path.dirname(__file__))
  os.makedirs('../app/images/icons/', exist_ok=True)
  if not os.path.exists('NotoSansSymbols-Regular.ttf'):
    subprocess.run([
        'wget',
        'https://noto-website.storage.googleapis.com/pkgs/NotoSansSymbols-unhinted.zip',
    ]).check_returncode()
    subprocess.run([
        'unzip',
        'NotoSansSymbols-unhinted.zip',
        'NotoSansSymbols-Regular.ttf',
    ]).check_returncode()
    os.remove('NotoSansSymbols-unhinted.zip')
  if not os.path.exists('MaterialIcons-Regular.ttf'):
    subprocess.run([
        'wget',
        'https://raw.githubusercontent.com/google/material-design-icons/master/iconfont/MaterialIcons-Regular.ttf',
    ]).check_returncode()
  if not os.path.exists('codepoints'):
    subprocess.run([
        'wget',
        'https://raw.githubusercontent.com/google/material-design-icons/master/iconfont/codepoints',
    ]).check_returncode()
  render('anchor')
  render('apl functional symbol quad up caret')
  render('black down-pointing triangle')
  render('black medium square')
  render('black question mark ornament')
  render('black truck')
  render('black up-pointing triangle')
  render('fountain')
  render('timeline')


if __name__ == '__main__':
  main()
