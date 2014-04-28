#!/usr/bin/env python
from __future__ import print_function
import subprocess
import sys

protected_branches = ["master"]
branch = subprocess.check_output("git rev-parse --abbrev-ref=strict HEAD".split()).strip()
if branch in protected_branches:
    print("You cannot commit directly to {0!r}. Please send a pull request.".format(branch), file=sys.stderr)
    sys.exit(1)
