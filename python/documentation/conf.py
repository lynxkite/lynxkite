import os
import re
import sys
from sphinx_lynx_theme import *
project = 'LynxKite Python API'

with open('../../../CHANGELOG.md') as f:
  changelog = f.read()
version = re.search(r'^### (\d+\.\S*)', changelog, re.MULTILINE).group(1)
release = version

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
]
sys.path.insert(0, os.path.abspath('..'))
