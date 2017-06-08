#!/usr/bin/env python3
'''Generates icons for all boxes.'''
import os
import PIL.Image
import PIL.ImageChops
import PIL.ImageOps
import subprocess
import unicodedata


def povray(output_file, caption, shadow_pass):
  with open('tmp.pov', 'w') as f:
    f.write('''
#version 3.7;
#include "scene.pov"
Statue("{}")
'''.format(caption))
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


def compose(output_file, caption):
  output_file = '../app/images/icons/' + output_file
  if os.path.exists(output_file):
    return
  povray('obj.png', caption, shadow_pass=0)
  povray('shadow.png', caption, shadow_pass=1)
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


def render(character):
  compose(character.replace(' ', '_') + '.png', unicodedata.lookup(character.upper()))


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
  render('anchor')
  render('apl functional symbol quad up caret')
  render('black down-pointing triangle')
  render('black medium square')
  render('black question mark ornament')
  render('black truck')
  render('black up-pointing triangle')
  render('fountain')


if __name__ == '__main__':
  main()
