#!/usr/bin/env python3
"""
Command-line utility to spin up an EMR cluster
(optionally with an RDS database), and run
LynxKite ecosystem on it.

Examples:

Launching cluster with 3 instances and using the current branch of
LynxKite ecosystem.

    ./emr_ecosystem.py

Launching cluster named `emr-lynx-cluster` with 5 instances
and using the native 1.9.10 version of LynxKite ecosystem.

    ./emr_ecosystem.py --cluster_name emr-lynx-cluster \
                       --emr_instance_count 5 \
                       --lynx_version native-1.9.10

"""

import os
import sys
# Set up import path for our modules.
os.chdir(os.path.dirname(__file__))
sys.path.append('remote_api/python')
from utils.ecosystem_lib import Ecosystem
from utils.ecosystem_lib import parser


def main(args):
  # cluster configuration
  cluster_config = {
      'cluster_name': args.cluster_name,
      'ec2_key_file': args.ec2_key_file,
      'ec2_key_name': args.ec2_key_name,
      'emr_region': args.emr_region,
      'emr_instance_count': args.emr_instance_count,
      'emr_log_uri': args.emr_log_uri,
      'hdfs_replication': '1',
      'with_rds': args.with_rds}

  # LynxKite configuration
  lynxkite_config = {
      'biggraph_releases_dir': args.biggraph_releases_dir,
      'lynx_version': args.lynx_version,
      'lynx_release_dir': args.lynx_release_dir,
      'log_dir': args.log_dir,
      's3_data_dir': args.s3_data_dir}

  # Launch cluster, start ecosystem and run tests.
  ecosystem = Ecosystem(cluster_config, lynxkite_config)
  ecosystem.launch_cluster()
  ecosystem.start()
  print('''Please don't forget to terminate the instances!''')


if __name__ == '__main__':
  args = parser.parse_args()
  main(args)
