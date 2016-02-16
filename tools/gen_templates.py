#!/usr/bin/python
import os
import re

header = re.compile("\[\[.*\]\]")
anchor = re.compile("\#\#\#")

def asciidocToTemplate(src, dst):
  codeBlock = False
  with open(src, 'r') as s:
    with open(dst, 'w') as d:
      for line in s:
        if line == '\`\`\`\n':
          codeBlock = not codeBlock
        else:
          if codeBlock:
            d.write(line)
          else:
            if line == '\n':
              d.write(line)
            else:
              if not header.match(line) and not anchor.match(line):
                d.write('# ' + line.replace('\`', ''))

app_home = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

asciidocToTemplate(
  app_home + '/web/app/admin_manual/installation/configuration/kiterc.asciidoc',
  app_home + '/conf/kiterc_template')

