#!/usr/bin/env python3
"""
Command-line utility to spin up an EMR cluster
(optionally with an RDS database), and run
LynxKite ecosystem on it.

Examples:

Launch a cluster with 3 instances and use the currently checked out code
of LynxKite ecosystem:

    ./emr_ecosystem.py


Launch a cluster named `emr-lynx-cluster` with 5 instances
and use the native 1.10.0 version of LynxKite ecosystem:

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
  # We don't want to stop a cluster which was just started.
  args.rm = False
  # Launch cluster and start ecosystem on it.
  ecosystem = Ecosystem(args)
  ecosystem.launch_cluster()
  ecosystem.start()
  if args.restore_metadata:
    ecosystem.restore_metadata()
  print('''Please don't forget to terminate the instances!''')


if __name__ == '__main__':
  args = arg_parser.parse_args()
  if args.s3_data_dir:
    assert args.s3_data_dir[-1] == '/', 'S3 bucket name has to end with "/".'
  if args.s3_metadata_version:
    assert args.s3_metadata_version.isdigit(), 'Version string can contain only digits.'
  main(args)
