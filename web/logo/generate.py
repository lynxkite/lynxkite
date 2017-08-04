#!/usr/bin/env python3
'''Generates the logo.'''
import os
import PIL.Image
import PIL.ImageChops
import PIL.ImageOps
import subprocess
import unicodedata
import yaml


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
      '+W500',  # Width.
      '+H100',  # Height.
      '+UA',  # Output alpha.
      '-D',  # No display.
      '+Itmp.pov',
      '+O' + output_file,
      'Declare=shadow_pass=' + str(shadow_pass),
  ]).check_returncode()
  os.remove('tmp.pov')


def compose(output_file, font, caption):
  output_file = '../app/images/' + output_file
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


def main():
  if os.path.dirname(__file__):
    os.chdir(os.path.dirname(__file__))
  if not os.path.exists('Exo2-Bold.ttf'):
    subprocess.run([
        'wget',
        'https://raw.githubusercontent.com/google/fonts/master/ofl/exo2/Exo2-Bold.ttf',
    ]).check_returncode()
  compose('logo.png', 'Exo2-Bold.ttf', 'LynxKite 2.0')


if __name__ == '__main__':
  main()
