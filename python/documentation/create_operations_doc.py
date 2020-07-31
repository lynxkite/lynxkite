''' Script for generating Python documentation from the ASCIIDOC documentation.

This script parses the .asciidoc documentation of the LynxKite Operations and
converts this into function descriptions that are stored in the operations.py
file in the Remote API. This file can be used to provide users from the Python
API with documentation regarding the operations available in LynxKite, either
through calling `help` or visiting the Sphinx documentation.

Usage: python create_operations_doc.py
'''
import glob
import os
import re
import textwrap

from lynx.kite import _python_name


FILES_TO_EXCLUDE = ['glossary.asciidoc', 'index.asciidoc']
ASCIIDOC_OPERATIONS_PATH = '../../web/app/help/operations/'
OUTPUT_PATH = '../remote_api/src/lynx/operations.py'
HEADER = f'''\'\'\' Python documentation for the operations in Lynxkite.
This document has been automatically generated.
\'\'\'
'''


def load_file(path):
  with open(path, 'r') as fh:
    return fh.read()


def format_italic(text):
  italic = re.compile(r'\_(?P<text>[a-zA-Z0-9 \-\+\*]*)\_')
  return italic.sub(r'*\g<text>*', text)


def generate_function(operation_name, path):
  ''' Generates the documentation for a given operation. '''
  content = load_file(path)

  # Delete 'include::g' tags
  content = re.sub(r'include::{g}\[tag=[A-Za-z0-9\-]*\]', '', content)
  # Replace experimental feature tags.
  content = re.sub(r'<<experimental.*>>', 'Experimental Feature\n', content)
  # Add arrow symbols
  content = re.sub(r'{to}', '→', content)
  content = re.sub(r'{from}', '←', content)
  # Format hyperlinks
  link_regex = re.compile(
      r'(?P<link>https?:\/\/[A-Za-z0-9.\-\/\#\*\_\(\)]*)\[(?P<name>[A-Za-z0-9 \-\:\_\.]*)\]')
  content = link_regex.sub(r'`\g<name> <\g<link>>`_', content)
  # Replace HTML symbols
  content = re.sub(r'&times;', '*', content)

  # Grep and format the description of the function.
  body = re.search(re.compile(r'^$.*(?P<body>[^=]*)', re.MULTILINE), content)
  body = textwrap.indent(body.groups()[0].strip(), '  ')
  body = format_italic(body)

  # Grep and format function parameters.
  params_regex = re.compile(r'\[p-(?P<attr>.*)\].*\n(?P<desc>[^=|\[]*)', re.MULTILINE)
  params = re.findall(params_regex, content)
  params = [] if not params else params
  params = [(name.replace('-', '_'), desc) for name, desc in params]
  params_str = ', '.join([name for name, desc in params])

  # Create parameter descriptions
  params_desc = ''
  for name, desc in params:
    desc = format_italic(desc.strip())
    params_desc += textwrap.indent(f':param {name}: {textwrap.indent(desc, "  ")}\n', '  ')
  params_desc = params_desc.rstrip()

  return f'''
def {operation_name}({params_str}):
  \'\'\'{body}

{params_desc}
  \'\'\'
'''


def get_python_name(file_name):
  '''Converts the file name into the corresponding function name.'''
  return _python_name(file_name[:-len('.asciidoc')].replace('-', ' '))


def generate_documentation(inpath, outpath):
  with open(outpath, 'w') as fh:
    fh.write(HEADER)

    for path in sorted(glob.glob(os.path.join(ASCIIDOC_OPERATIONS_PATH, '*.asciidoc'))):
      file_name = os.path.basename(path)
      if file_name in FILES_TO_EXCLUDE:
        continue
      operation_name = get_python_name(file_name)
      fh.write(generate_function(operation_name, path))


if __name__ == '__main__':
  generate_documentation(ASCIIDOC_OPERATIONS_PATH, OUTPUT_PATH)
