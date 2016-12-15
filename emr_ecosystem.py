#!/usr/bin/env python3
"""
Command-line utility to spin up an EMR cluster
(optionally with an RDS database), and run
LynxKite ecosystem on it.

Examples:

Launching cluster with 3 instances and using the currently checked out code
of LynxKite ecosystem:

    ./emr_ecosystem.py


Launching cluster named `emr-lynx-cluster` with 5 instances
and using the native 1.10.0 version of LynxKite ecosystem:

    ./emr_ecosystem.py --cluster_name emr-lynx-cluster \
                       --emr_instance_count 5 \
                       --lynx_version native-1.10.0

"""

import os
import sys
# Set up import path for our modules.
os.chdir(os.path.dirname(__file__))
sys.path.append('remote_api/python')
from utils.ecosystem_lib import Ecosystem
from utils.ecosystem_lib import arg_parser

arg_parser.add_argument(
    '--emr_instance_count',
    type=int,
    default=3,
    help='Number of instances on EMR cluster, including master.')


def main(args):
  # We don't want to stop cluster which was just started.
  args.rm = False
  # Launch cluster and start ecosystem on it.
  ecosystem = Ecosystem(args)
  ecosystem.launch_cluster()
  ecosystem.start()
  print('''Please don't forget to terminate the instances!''')


if __name__ == '__main__':
  args = arg_parser.parse_args()
  main(args)
