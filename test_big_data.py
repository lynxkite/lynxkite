#!/usr/bin/env python3
"""
Command-line utility to spin up an EMR cluster
(optionally with an RDS database), and run
Luigi task based performance tests on it.

Examples:

Running the default big data tests on the small data set using
the currently checked out branch of the native release:

    ./test_big_data.py


Running all big data tests on the medium data set using the
currently checked out branch:

    ./test_big_data.py  --test_set_size medium --task AllTests


Running JDBC tests on a cluster named `JDBC-test-cluster` using version 1.9.5:

    ./test_big_data.py  --cluster_name JDBC-test-cluster \
                        --with_rds --lynx_version native-1.9.5 \
                        --task_module test_tasks.jdbc --task JDBCTestAll


Running ModularClustering test on the large data set using the currently
checked out branch and downloading application logs from the
cluster to `/home/user/cluster-logs`:

    ./test_big_data.py  --test_set_size large --task ModularClustering \
                        --log_dir /home/user/cluster-logs


Running the default big data tests on the medium data set using a cluster
with 6 nodes (1 master, 5 worker) and currently checked out branch of the
native release:

    ./test_big_data.py  --test_set_size medium --emr_instance_count 6
"""

import os
import sys
# Set up import path for our modules.
os.chdir(os.path.dirname(__file__))
sys.path.append('remote_api/python')
from utils.ecosystem_lib import Ecosystem
from utils.ecosystem_lib import arg_parser

#  Big data test sets in the  `s3://lynxkite-test-data/` bucket.
#  fake_westeros_v3_100k_2m     100k vertices, 2m edges (small)
#  fake_westeros_v3_5m_145m     5m vertices, 145m edges (medium)
#  fake_westeros_v3_10m_303m    10m vertices, 303m edges (large)
#  fake_westeros_v3_25m_799m    25m vertices 799m edges (xlarge)

test_sets = {
    'small': dict(data='fake_westeros_v3_100k_2m', instances=3),
    'medium': dict(data='fake_westeros_v3_5m_145m', instances=4),
    'large': dict(data='fake_westeros_v3_10m_303m', instances=4),
    'xlarge': dict(data='fake_westeros_v3_25m_799m', instances=8),
}


arg_parser.add_argument(
    '--emr_instance_count',
    type=int,
    default=0,
    help='Number of instances on EMR cluster, including master.' +
    ' Set according to dataset and test_sets by default.')
arg_parser.add_argument(
    '--rm',
    action='store_true',
    help='''Delete the cluster after completion.''')
arg_parser.add_argument(
    '--task_module',
    default='test_tasks.bigdata_tests',
    help='Module of the luigi task which will run on the cluster.')
arg_parser.add_argument(
    '--task',
    default='DefaultTests',
    help='Luigi task to run when the cluster is started.')
arg_parser.add_argument(
    '--results_dir',
    default='./ecosystem/tests/results/',
    help='Test results are downloaded to this directory.')
arg_parser.add_argument(
    '--test_set_size',
    default='small',
    help='Test set for big data tests. Possible values: small, medium, large, xlarge.')


def main(args):
  # Cluster config for tests

  args.with_jupyter = False
  if args.emr_instance_count == 0:
    args.emr_instance_count = test_sets[args.test_set_size]['instances']
  # Test configuration
  test_config = {
      'task_module': args.task_module,
      'task': args.task,
      'dataset': test_sets[args.test_set_size]['data'],
      'results_local_dir': results_local_dir(args),
      'results_name': "/{task}-result.txt".format(task=args.task)}
  # Launch cluster, start ecosystem and run tests.
  ecosystem = Ecosystem(args)
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
  dataset = test_sets[args.test_set_size]['data']
  instance_count = args.emr_instance_count
  executors = instance_count - 1
  return "{bd}emr_{e}_{i}_{ds}".format(
      bd=basedir,
      e=executors,
      i=instance_count,
      ds=dataset,
  )


if __name__ == '__main__':
  args = arg_parser.parse_args()
  main(args)
