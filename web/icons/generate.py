#!/usr/bin/env python3
'''Generates icons for all boxes.'''
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


def lookup_fontawesome_codepoint(name, registry={}):
  if not registry:
    with open('icons.yml') as f:
      fa = yaml.load(f.read())
    for i in fa['icons']:
      ch = chr(int(i['unicode'], 16))
      registry[i['id']] = ch
  return registry[name]


def render(name):
  try:
    font = 'fontawesome-webfont.ttf'
    character = lookup_fontawesome_codepoint(name)
  except KeyError:  # Not a Font Awesome character name.
    font = 'NotoSansSymbols-Regular.ttf'
    character = unicodedata.lookup(name.upper())
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
  if not os.path.exists('fontawesome-webfont.ttf'):
    subprocess.run([
        'wget',
        'https://raw.githubusercontent.com/FortAwesome/Font-Awesome/v4.7.0/fonts/fontawesome-webfont.ttf',
    ]).check_returncode()
  if not os.path.exists('icons.yml'):
    subprocess.run([
        'wget',
        'https://raw.githubusercontent.com/FortAwesome/Font-Awesome/v4.7.0/src/icons.yml',
    ]).check_returncode()
  render('anchor')
  render('black down-pointing triangle')
  render('black up-pointing triangle')
  render('black medium square')
  render('black question mark ornament')
  render('truck')
  render('download')
  render('upload')
  render('gavel')
  render('filter')
  render('th-large')
  render('globe')
  render('asterisk')
  render('circle')
  render('share-alt')
  render('podcast')
  render('signal')
  render('android')
  render('wrench')
  render('eye')
  render('superpowers')
  render('snowflake-o')
  render('cogs')


if __name__ == '__main__':
  main()
