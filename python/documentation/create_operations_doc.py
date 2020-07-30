''' Script for generating Python documentation from the ASCIIDOC documentation.



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

BODY_REGEX = re.compile(r'^$.*(?P<body>[^=]*)', re.MULTILINE)
PARAMS_REGEX = re.compile(r'\[p-(?P<attr>.*)\].*\n(?P<desc>[^=|\[]*)', re.MULTILINE)


def load_file(path):
  with open(path, 'r') as fh:
    return fh.read()


def generate_function(operation_name, path):
  ''' Generates the documentation for a given operation. '''
  content = load_file(path)

  body = textwrap.indent(re.search(BODY_REGEX, content).groups()[0].strip(), '  ')
  params = re.findall(PARAMS_REGEX, content)
  params = [] if not params else params
  params = [(name.replace('-', '_'), desc) for name, desc in params]
  params_str = ', '.join([name for name, desc in params])

  params_text = ''
  for name, desc in params:
    params_text += textwrap.indent(f':param {name}: {textwrap.indent(desc.strip(), "  ")}\n', '  ')
  params_text = params_text.rstrip()
  return f'''
def {operation_name}({params_str}):
  \'\'\'{body}

{params_text}
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
