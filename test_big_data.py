#!/usr/bin/env python3
"""
Command-line utility to spin up an EMR cluster
(optionally with an RDS database), and run
Luigi task based performance tests on it.

Examples:

Running the default big data tests on the small data set using
the current branch of the native release.

    ./test_big_data.py

Running all big data tests on the medium data set using the current branch.

    ./test_big_data.py  --dataset medium --task AllTests

Running JDBC tests on a cluster named `JDBC-test-cluster` using version 1.9.5.

    ./test_big_data.py  --cluster_name JDBC-test-cluster \
                        --with_rds --lynx_version native-1.9.5 \
                        --task_module test_tasks.jdbc --task JDBCTestAll

Running ModularClustering test on the large data set using the current branch
and downloading application logs from the cluster to `/home/user/cluster-logs`.

    ./test_big_data.py  --dataset large --task ModularClustering \
                        --log_dir /home/user/cluster-logs

Running the default big data tests on the medium data set using a cluster
with 6 nodes (1 master, 5 worker) and current branch of the native release.

    ./test_big_data.py  --dataset medium --emr_instance_count 6
"""

import os
import sys
# Set up import path for our modules.
os.chdir(os.path.dirname(__file__))
sys.path.append('remote_api/python')
from utils.ecosystem_lib import Ecosystem
from utils.ecosystem_lib import parser

#  Big data test sets in the  `s3://lynxkite-test-data/` bucket.
#  fake_westeros_v3_100k_2m     100k vertices, 2m edges (small)
#  fake_westeros_v3_5m_145m     5m vertices, 145m edges (medium)
#  fake_westeros_v3_10m_303m    10m vertices, 303m edges (large)
#  fake_westeros_v3_25m_799m    25m vertices 799m edges (xlarge)

test_sets = {
    'small': dict(data='fake_westeros_v3_100k_2m', instances=3),
    'medium': dict(data='fake_westeros_v3_5m_145m', instances=4),
    'large': dict(data='fake_westeros_v3_10m_303m', instances=8),
    'xlarge': dict(data='fake_westeros_v3_25m_799m', instances=20),
}


parser.add_argument(
    '--rm',
    action='store_true',
    help='''Delete the cluster after completion.''')
parser.add_argument(
    '--task_module',
    default='test_tasks.bigdata_tests',
    help='Module of the luigi task which will run on the cluster.')
parser.add_argument(
    '--task',
    default='DefaultTests',
    help='Luigi task to run when the cluster is started.')
parser.add_argument(
    '--results_dir',
    default='./ecosystem/tests/results/',
    help='Test results are downloaded to this directory.')
parser.add_argument(
    '--dataset',
    default='small',
    help='Test set for big data tests. Possible values: small, medium, large, xlarge.')


def main(args):
  # cluster configuration
  cluster_config = {
      'cluster_name': args.cluster_name,
      'ec2_key_file': args.ec2_key_file,
      'ec2_key_name': args.ec2_key_name,
      'emr_region': args.emr_region,
      'emr_instance_count': test_sets[args.dataset]['instances'],
      'emr_log_uri': args.emr_log_uri,
      'hdfs_replication': '1',
      'with_rds': args.with_rds,
      'rm': args.rm}

  # LynxKite configuration
  lynxkite_config = {
      'biggraph_releases_dir': args.biggraph_releases_dir,
      'lynx_version': args.lynx_version,
      'lynx_release_dir': args.lynx_release_dir,
      'task_module': args.task_module,
      'task': args.task,
      'log_dir': args.log_dir,
      's3_data_dir': args.s3_data_dir}

  # Test configuration
  test_config = {
      'dataset': test_sets[args.dataset]['data'],
      'results_local_dir': results_local_dir(args),
      'results_name': results_name(args)}

  # Launch cluster, start ecosystem and run tests.
  ecosystem = Ecosystem(cluster_config, lynxkite_config)
  ecosystem.launch_cluster()
  ecosystem.start()
  ecosystem.run_tests(test_config)
  ecosystem.cleanup()


def results_local_dir(args):
  '''
  In case of big data tests, the name of the result dir includes the number of instances,
  the number of executors and the name of the test data set.
  '''
  basedir = args.results_dir
  dataset = test_sets[args.dataset]['data']
  instance_count = test_sets[args.dataset]['instances']
  executors = instance_count - 1
  return "{bd}emr_{e}_{i}_{ds}".format(
      bd=basedir,
      e=executors,
      i=instance_count,
      ds=dataset,
  )


def results_name(args):
  return "/{task}-result.txt".format(task=args.task)


if __name__ == '__main__':
  args = parser.parse_args()
  main(args)
