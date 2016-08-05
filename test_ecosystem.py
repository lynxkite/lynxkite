#!/usr/bin/python3

import argparse
import boto3
import os
import time
from utils.emr_lib import EMRLib

parser = argparse.ArgumentParser()
parser.add_argument(
    '--cluster_name',
    default=os.environ['USER'] + '-boto-cluster',
    help='Name of the cluster to start')
parser.add_argument(
    '--biggraph_releases_dir',
    default=os.environ['HOME'] + '/biggraph_releases',
    help='''Directory containing the downloader script, typically the root of
         the biggraph_releases repo. The downloader script will have the form of
         BIGGRAPH_RELEASES_DIR/download-lynx-LYNX_VERSION.sh''')
parser.add_argument(
    '--lynx_version',
    default='1.9.0',
    help='''Version of the ecosystem release to test. A downloader script of the
          following form will be used for obtaining the release:
         BIGGRAPH_RELEASES_DIR/download-lynx-LYNX_VERSION.sh''')
parser.add_argument(
    '--ec2_key_file',
    default=os.environ['HOME'] + '/.ssh/lynx-cli.pem')
parser.add_argument(
    '--ec2_key_name',
    default='lynx-cli')


def main(args):
  # Create EMR cluster and MySQL database in RDS.
  lib = EMRLib(
      ec2_key_file=args.ec2_key_file,
      ec2_key_name=args.ec2_key_name)
  cluster = lib.create_or_connect_to_emr_cluster(name=args.cluster_name)
  mysql_instance = lib.create_or_connect_to_rds_instance(
      name=args.cluster_name + '-mysql')
  # Wait for startup of both.
  lib.wait_for_services([cluster, mysql_instance])

  mysql_address = mysql_instance.get_address()
  jdbc_url = 'jdbc:mysql://{mysql_address!s}/db?user=root&password=rootroot'.format(
      mysql_address=mysql_address)

  # Upload installer script.
  cluster.rsync_up(
      src='{dir!s}/download-lynx-{version!s}.sh'.format(
          dir=args.biggraph_releases_dir,
          version=args.lynx_version),
      dst='/mnt/')

  # Upload tasks.
  ecosystem_task_dir = '/mnt/lynx-' + args.lynx_version + '/lynx/luigi_tasks/test_tasks'
  cluster.ssh('mkdir -p ' + ecosystem_task_dir)
  cluster.rsync_up('ecosystem/tests/', ecosystem_task_dir)

  install_docker_and_lynx(cluster, args.lynx_version)
  start_or_reset_ecosystem(cluster, args.lynx_version)
  start_tests(cluster, jdbc_url)
  print('Tests are now running in the background. Waiting for results.')
  process_output(cluster)
  # TODO: shut down cluster here
  print("Please don't forget to shut down the cluster!")


def install_docker_and_lynx(cluster, version):
  cluster.ssh('''
    set -x
    cd /mnt

    if [ ! -f "./lynx-{version!s}/lynx/start.sh" ]; then
      # Not yet installed.
      ./download-lynx-{version!s}.sh
      tar xfz lynx-{version!s}.tgz
    fi

    cd lynx-{version!s}

    if ! hash docker 2>/dev/null; then
      echo "docker not found, installing"
      sudo LYNXKITE_USER=hadoop ./install-docker.sh
    fi

    # no need for scheduled tasks today
    echo "sleep_period_seconds: 1000000000\nscheduling: []" >lynx/luigi_tasks/chronomaster.yml

    # Log out here to refresh docker user group.
  '''.format(version=version))


def start_or_reset_ecosystem(cluster, version):
  res = cluster.ssh('''
    cd /mnt/lynx-{version!s}/lynx
    # Start ecosystem.
    if [[ $(docker ps -qf name=lynx_luigi_worker_1) ]]; then
      echo "Ecosystem is running. Cleanup and Luigi restart is needed."
      hadoop fs -rm -r /user/hadoop/lynxkite_data
      sudo rm -Rf ./lynxkite_metadata/*
      ./reload_luigi_tasks.sh
    else
      ./start.sh
    fi

    # Wait for ecosystem startup completion.
    # (We keep retrying a dummy task until it succeeds.)
    docker exec lynx_luigi_worker_1 rm -f /tasks_data/smoke_test_marker.txt
    while [[ $(docker exec lynx_luigi_worker_1 cat /tasks_data/smoke_test_marker.txt) != "done" ]]; do
      docker exec lynx_luigi_worker_1 luigi --module test_tasks.smoke_test SmokeTest 2>/dev/null
      sleep 1
    done'''.format(version=args.lynx_version))


def start_tests(cluster, jdbc_url):
  cluster.ssh('''
    cat >/home/hadoop/run_test.sh <<EOF
      docker exec lynx_luigi_worker_1 luigi --module test_tasks.jdbc JDBCTest --jdbc-url '{jdbc_url!s}'
      echo 'done' >/home/hadoop/test_status.txt
    echo
EOF
    chmod a+x /home/hadoop/run_test.sh
    rm -f /home/hadoop/test_status.txt
    nohup /home/hadoop/run_test.sh >/home/hadoop/test_results.txt 2>&1 &
  '''.format(jdbc_url=jdbc_url))


def process_output(cluster):
  output_lines_seen = 0
  status_is_done = False
  while not status_is_done:
    # Check status.
    status_is_done = 'done' == cluster.ssh(
        'cat /home/hadoop/test_status.txt 2>/dev/null',
        print_output=False,
        verbose=False).strip()
    # Print unseen log lines.
    output_results = cluster.ssh(
        'tail -n +{first_line!s} /home/hadoop/test_results.txt'.format(
            first_line=output_lines_seen + 1),
        verbose=False)
    output_lines_seen += output_results.count('\n')
    time.sleep(5)


if __name__ == '__main__':
  args = parser.parse_args()
  main(args)
