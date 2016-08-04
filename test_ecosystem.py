#!/usr/bin/python3

import argparse
import boto3
import os
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
  lib = EMRLib(
      ec2_key_file=args.ec2_key_file,
      ec2_key_name=args.ec2_key_name)
  cluster = lib.create_or_connect(name=args.cluster_name)
  mysql_instance = lib.create_or_connect_db(
      name=args.cluster_name + '-mysql')

  lib.wait_for_services([cluster, mysql_instance])

  mysql_address = mysql_instance.get_address()
  jdbc_url = 'jdbc:mysql://{mysql_address!s}/db?user=root&password=rootroot'.format(
      mysql_address=mysql_address)

  ecosystem_base_dir = '/mnt/lynx-' + args.lynx_version
  ecosystem_home_dir = ecosystem_base_dir + '/lynx'
  ecosystem_task_dir = ecosystem_home_dir + '/luigi_tasks/test_tasks'

  # Upload installer script.
  cluster.rsync_up(
      src='{dir!s}/download-lynx-{version!s}.sh'.format(
          dir=args.biggraph_releases_dir,
          version=args.lynx_version),
      dst='/mnt/')

  # Upload tasks.
  cluster.ssh('mkdir -p ' + ecosystem_task_dir)
  cluster.rsync_up('ecosystem/tests/', ecosystem_task_dir)

  # Install Docker and LynxKite ecosystem.
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
    echo "scheduling: []" >lynx/luigi_tasks/chronomaster.yml

    # log out to refresh docker user group
  '''.format(version=args.lynx_version))
  cluster.ssh('''
    cd /mnt/lynx-{version!s}/lynx
    if [[ $(docker ps -qf name=lynx_luigi_worker_1) ]]; then
      echo "Ecosystem is running. Cleanup and Luigi restart is needed."
      hadoop fs -rm -r /user/hadoop/lynxkite_data
      sudo rm -Rf ./lynxkite_metadata/*
      ./reload_luigi_tasks.sh
    else
      ./start.sh
    fi

    docker exec -i lynx_luigi_worker_1 luigi \
      --module test_tasks.smoke_test \
      JDBCTest \
      --jdbc-url '{jdbc_url!s}' \

  '''.format(jdbc_url=jdbc_url, version=args.lynx_version))

if __name__ == '__main__':
  args = parser.parse_args()
  main(args)
