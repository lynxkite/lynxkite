'''Copies the smallest emoji images from Noto Emoji.'''
import json
import os
import shutil
import unicodedata

NOTO_EMOJI = os.environ['HOME'] + '/noto-emoji'
DESTINATION = os.path.dirname(os.path.realpath(__file__))
files = []
for png in os.scandir(NOTO_EMOJI + '/png/128'):
  if (not png.name.startswith('emoji_') or
      not png.name.endswith('.png') or
          len(png.name.split('_')) == 3):
    continue
  size = png.stat().st_size
  code = png.name.split('_')[1].split('.')[0].replace('u', '')
  try:
    name = unicodedata.name(chr(int(code, 16))).lower().replace(' ', '_')
    if ('squared' in name or
        'modifier' in name or
        'regional_indicator' in name or
        'digit_' in name or
        'number_sign' in name or
            'asterisk' in name):
      continue
    files.append((size, code, png.name, name))
  except:
    pass

files.sort()
files = sorted([t[1:] for t in files[:600]])
files = [t[1:] for t in files]

for fn, name in files:
  shutil.copyfile(NOTO_EMOJI + '/png/128/' + fn, DESTINATION + '/' + name + '.png')

files = [t[1] for t in files]
with open(DESTINATION + '/list.json', 'w') as f:
  f.write(json.dumps(files))
