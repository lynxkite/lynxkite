#!/usr/bin/env python3
"""
Command-line utility to spin up an EMR cluster
(optionally with an RDS database), and run
LynxKite ecosystem on it.

Examples:

Running all big data tests on the normal data set using the current branch.

    ./emr_ecosystem.py


"""
import argparse
import os
import sys
# Set up import path for our modules.
os.chdir(os.path.dirname(__file__))
sys.path.append('remote_api/python')
from utils.ecosystem_lib import Ecosystem


parser = argparse.ArgumentParser()
parser.add_argument(
  '--cluster_name',
  default=os.environ['USER'] + '-ecosystem-test',
  help='Name of the cluster to start')
parser.add_argument(
  '--ec2_key_file',
  default=os.environ['HOME'] + '/.ssh/lynx-cli.pem')
parser.add_argument(
  '--ec2_key_name',
  default='lynx-cli')
parser.add_argument(
  '--emr_region',
  default='us-east-1',
  help='Region of the EMR cluster.' +
       ' Possible values: us-east-1, ap-southeast-1, eu-central-1, ...')
parser.add_argument(
  '--emr_instance_count',
  type=int,
  default=3,
  help='Number of instances on EMR cluster, including master.' +
       ' Set according to bigdata_test_set by default.')
parser.add_argument(
  '--emr_log_uri',
  default='s3://test-ecosystem-log',
  help='URI of the S3 bucket where the EMR logs will be written.')
parser.add_argument(
  '--with_rds',
  action='store_true',
  help='Spin up a mysql RDS instance to test database operations.')
parser.add_argument(
  '--biggraph_releases_dir',
  default=os.environ['HOME'] + '/biggraph_releases',
  help='''Directory containing the downloader script, typically the root of
         the biggraph_releases repo. The downloader script will have the form of
         BIGGRAPH_RELEASES_DIR/download-lynx-LYNX_VERSION.sh''')
parser.add_argument(
  '--lynx_version',
  default='native-1.10.0',
  help='''Version of the ecosystem release to test. A downloader script of the
          following form will be used for obtaining the release:
         BIGGRAPH_RELEASES_DIR/download-lynx-LYNX_VERSION.sh''')
parser.add_argument(
  '--lynx_release_dir',
  default='',
  help='''If non-empty, then this local directory is directly uploaded instead of
         using LYNX_VERSION and BIGGRAPH_RELEASES_DIR. The directory of the current
         native code is ecosystem/native/dist.''')
parser.add_argument(
  '--task_module',
  default='test_tasks.bigdata_tests',
  help='Module of the luigi task which will run on the cluster.')
parser.add_argument(
  '--task',
  default='DefaultTests',
  help='Luigi task to run when the cluster is started.')
parser.add_argument(
  '--log_dir',
  default='',
  help='''Cluster log files are downloaded to this directory.
    If it is an empty string, no log file is downloaded.''')
parser.add_argument(
  '--s3_data_dir',
  help='S3 path to be used as non-ephemeral data directory.')


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
    'task_module': args.task_module,
    'task': args.task,
    'log_dir': args.log_dir,
    's3_data_dir': args.s3_data_dir}

  # Launch cluster, start ecosystem and run tests.
  ecosystem = Ecosystem(cluster_config, lynxkite_config)
  ecosystem.launch_cluster()
  ecosystem.start()


if __name__ == '__main__':
  args = parser.parse_args()
  main(args)
