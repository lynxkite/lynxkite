#!/usr/bin/env python
'''Generates configuration template files from AsciiDoc sources.

The idea is that this way the same documentation can be present in the file, and also in the
administrator's guide.
'''
import os
import re

header = re.compile(r'\[\[.*\]\]')
anchor = re.compile(r'\#\#\#')
url = re.compile(r'(http.*)\[(.*)\]')
bold = re.compile(r'\*([^`]*)\*')


def asciidocToTemplate(src, dst):
  codeBlock = False
  with open(src, 'r') as s:
    with open(dst, 'w') as d:
      for line in s:
        if line == '```\n':
          codeBlock = not codeBlock
        else:
          if codeBlock:
            d.write(line)
          else:
            if line == '\n':
              d.write(line)
            else:
              if not header.match(line) and not anchor.match(line):
                line = url.sub(r'\2 (\1)', line)
                line = bold.sub(r'\1', line)
                line = line.replace('`', '')
                d.write('# ' + line)

app_home = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

asciidocToTemplate(
    app_home + '/web/app/admin-manual/installation/configuration/kiterc.asciidoc',
    app_home + '/conf/kiterc_template')
asciidocToTemplate(
    app_home + '/web/app/admin-manual/installation/configuration/emr.asciidoc',
    app_home + '/tools/emr_spec_template')
asciidocToTemplate(
    app_home + '/web/app/admin-manual/installation/configuration/prefix_definitions.asciidoc',
    app_home + '/conf/prefix_definitions_template.txt')
