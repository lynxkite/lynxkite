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
      '+Itmp.pov',
      '+O' + output_file,
      'Declare=shadow_pass=' + str(shadow_pass),
  ]).check_returncode()


def compose(output_file, caption):
  output_file = '../dist/icons/' + output_file
  if os.path.exists(output_file):
    return
  povray('obj.png', caption, 0)
  povray('shadow.png', caption, 1)
  obj = PIL.Image.open('obj.png')
  shadow = PIL.Image.open('shadow.png').convert('L')
  shadow = shadow.point(lambda x: 1.1 * x)
  unshadow = PIL.ImageChops.invert(shadow)
  black = unshadow.copy()
  black.paste(0)
  shadow = PIL.Image.merge('RGBA', (black, black, black, unshadow))
  PIL.Image.alpha_composite(shadow, obj).save(output_file, 'png')


def render(character):
  compose(character.replace(' ', '_') + '.png', unicodedata.lookup(character.upper()))


def main():
  if os.path.dirname(__file__):
    os.chdir(os.path.dirname(__file__))
  os.makedirs('../dist/icons', exist_ok=True)
  render('anchor')
  render('black question mark ornament')
  render('black medium square')
  render('black down-pointing triangle')
  render('black up-pointing triangle')
  render('black truck')
  render('apl functional symbol quad up caret')


if __name__ == '__main__':
  main()
