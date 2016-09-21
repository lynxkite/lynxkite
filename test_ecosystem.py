#!/usr/bin/env python3
'''
Command-line utility to spin up an EMR cluster with an RDS
database, and run Luigi task based performance tests on it.
'''
import argparse
import boto3
import os
import time
import sys
# Set up import path for our modules.
os.chdir(os.path.dirname(__file__))
sys.path.append('remote_api/python')
from utils.emr_lib import EMRLib

parser = argparse.ArgumentParser()
parser.add_argument(
    '--cluster_name',
    default=os.environ['USER'] + '-ecosystem-test',
    help='Name of the cluster to start')
parser.add_argument(
    '--biggraph_releases_dir',
    default=os.environ['HOME'] + '/biggraph_releases',
    help='''Directory containing the downloader script, typically the root of
         the biggraph_releases repo. The downloader script will have the form of
         BIGGRAPH_RELEASES_DIR/download-lynx-LYNX_VERSION.sh''')
parser.add_argument(
    '--lynx_version',
    default='native-1.9.3',
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
    default='test_tasks.jdbc')
parser.add_argument(
    '--task',
    default='JDBCTest1k')
parser.add_argument(
    '--ec2_key_file',
    default=os.environ['HOME'] + '/.ssh/lynx-cli.pem')
parser.add_argument(
    '--ec2_key_name',
    default='lynx-cli')
parser.add_argument(
    '--emr_instance_count',
    default=3)
parser.add_argument(
    '--results_dir',
    default='./ecosystem/tests/results/')
parser.add_argument(
    '--rm',
    action='store_true',
    help='''Delete the cluster after completion.''')
parser.add_argument(
    '--dockerized',
    action='store_true')


def main(args):
  # Create an EMR cluster and a MySQL database in RDS.
  lib = EMRLib(
      ec2_key_file=args.ec2_key_file,
      ec2_key_name=args.ec2_key_name)
  cluster = lib.create_or_connect_to_emr_cluster(
      name=args.cluster_name,
      instance_count=args.emr_instance_count)
  mysql_instance = lib.create_or_connect_to_rds_instance(
      name=args.cluster_name + '-mysql')
  # Wait for startup of both.
  lib.wait_for_services([cluster, mysql_instance])

  mysql_address = mysql_instance.get_address()
  jdbc_url = 'jdbc:mysql://{mysql_address!s}/db?user=root&password=rootroot'.format(
      mysql_address=mysql_address)

  upload_installer_script(cluster, args)
  upload_tasks(cluster, args)
  download_and_unpack_release(cluster, args)
  if args.dockerized:
    install_docker_and_lynx(cluster, args.lynx_version)
    # TODO: config_aws_s3_docker(cluster)
    start_or_reset_ecosystem_docker(cluster, args.lynx_version)
    start_tests_docker(cluster, jdbc_url)
  else:
    install_native(cluster)
    config_and_prepare_native(cluster, args)
    config_aws_s3_native(cluster)
    start_supervisor_native(cluster)
    start_tests_native(cluster, jdbc_url, args)
  print('Tests are now running in the background. Waiting for results.')
  cluster.fetch_output()
  cluster.rsync_down('/home/hadoop/test_results.txt', args.results_dir + '/result.txt')
  shut_down_instances(cluster, mysql_instance)


def upload_installer_script(cluster, args):
  if not args.lynx_release_dir:
    cluster.rsync_up(
        src='{dir!s}/download-lynx-{version!s}.sh'.format(
            dir=args.biggraph_releases_dir,
            version=args.lynx_version),
        dst='/mnt/')


def upload_tasks(cluster, args):
  if args.dockerized:
    ecosystem_task_dir = '/mnt/lynx/lynx/luigi_tasks/test_tasks'
  else:
    ecosystem_task_dir = '/mnt/lynx/luigi_tasks/test_tasks'
  cluster.ssh('mkdir -p ' + ecosystem_task_dir)
  cluster.rsync_up('ecosystem/tests/', ecosystem_task_dir)
  if not args.dockerized:
    cluster.ssh('''
        set -x
        cd /mnt/lynx/luigi_tasks/test_tasks
        mv test_runner.py /mnt/lynx/luigi_tasks
        ''')


def install_native(cluster):
  cluster.ssh('''
    set -x
    cd /mnt/lynx
    sudo yum install -y python34-pip mysql-server gcc libffi-devel
    sudo pip-3.4 install --upgrade luigi sqlalchemy mysqlclient PyYAML prometheus_client
    sudo pip-2.6 install --upgrade requests[security] supervisor
    # mysql setup
    sudo service mysqld start
    mysqladmin  -u root password 'root'
    #remote access for the executors
    mysql -uroot -proot -e "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'root'"
  ''')


def config_and_prepare_native(cluster, args):
  cluster.ssh('''
    cd /mnt/lynx
    # Dirty solution because kiterc keeps growing:
    echo 'Setting up environment variables.'
    #http://stackoverflow.com/questions/5227295/how-do-i-delete-all-lines-in-a-file-starting-from-after-a-matching-line
    sed -i -n '/# ---- the below lines were added by test_ecosystem.py ----/q;p'  config/central
    cat >>config/central <<'EOF'
# ---- the below lines were added by test_ecosystem.py ----
      export KITE_INSTANCE=ecosystem-test
      export KITE_MASTER_MEMORY_MB=8000
      export NUM_EXECUTORS={num_executors!s}
      export EXECUTOR_MEMORY=18g
      export NUM_CORES_PER_EXECUTOR=8
      # port differs from the one used in central/config
      export HDFS_ROOT=hdfs://$HOSTNAME:8020/user/$USER
      export KITE_DATA_DIR=hdfs://$HOSTNAME:8020/user/$USER/lynxkite/
      export LYNXKITE_ADDRESS=https://localhost:$KITE_HTTPS_PORT/
      export PYTHONPATH=/mnt/lynx/apps/remote_api/python/:/mnt/lynx/luigi_tasks
      export HADOOP_CONF_DIR=/etc/hadoop/conf
      export LYNX=/mnt/lynx
      #for tests with mysql server on master
      export DATA_DB=jdbc:mysql://$HOSTNAME:3306/'db?user=root&password=root&rewriteBatchedStatements=true'
EOF
    echo 'Creating hdfs directory.'
    source config/central
    hdfs dfs -mkdir -p $KITE_DATA_DIR/table_files
    echo 'Creating tasks_data directory.'
    # TODO: Find a more sane directory.
    sudo mkdir /tasks_data
    sudo chmod a+rwx /tasks_data
  '''.format(num_executors=args.emr_instance_count - 1))


def config_aws_s3_native(cluster):
  cluster.ssh('''
    cd /mnt/lynx
    echo 'Setting s3 prefix.'
    cat >>config/prefix_definitions.txt <<'EOF'
S3="s3://"
EOF
    echo 'Setting AWS CLASSPATH.'
    cat >>spark/conf/spark-env.sh <<'EOF'
AWS_CLASSPATH1=$(find /usr/share/aws/emr/emrfs/lib -name "*.jar" | tr '\\n' ':')
AWS_CLASSPATH2=$(find /usr/share/aws/aws-java-sdk -name "*.jar" | tr '\\n' ':')
AWS_CLASSPATH3=$(find /usr/share/aws/emr/instance-controller/lib -name "*.jar" | tr '\\n' ':')
AWS_CLASSPATH_ALL=$AWS_CLASSPATH1$AWS_CLASSPATH2$AWS_CLASSPATH3
export SPARK_DIST_CLASSPATH=$SPARK_DIST_CLASSPATH:${AWS_CLASSPATH_ALL::-1}
EOF
    chmod a+x spark/conf/spark-env.sh
  ''')


def start_supervisor_native(cluster):
  cluster.ssh_nohup('''
    set -x
    source /mnt/lynx/config/central
    /usr/local/bin/supervisord -c config/supervisord.conf
    ''')


def start_tests_native(cluster, jdbc_url, args):
  '''Start running the tests in the background.'''
  cluster.ssh_nohup('''
      source /mnt/lynx/config/central
      echo 'cleaning up previous test data'
      cd /mnt/lynx
      hadoop fs -rm -r /user/hadoop/lynxkite
      sudo rm -Rf metadata/lynxkite/*
      supervisorctl restart lynxkite
      ./reload_luigi_tasks.sh
      echo 'Waiting for the ecosystem to start...'
      rm -f /tasks_data/smoke_test_marker.txt
      rm -Rf /tmp/luigi/
      touch /mnt/lynx/luigi_tasks/test_tasks/__init__.py
      while [[ $(cat /tasks_data/smoke_test_marker.txt 2>/dev/null) != "done" ]]; do
        luigi --module test_tasks.smoke_test SmokeTest
        sleep 1
      done
      echo 'Ecosystem started.'
      python3 /mnt/lynx/luigi_tasks/test_runner.py \
          --module {luigi_module!s} \
          --task {luigi_task!s} \
          --task-param jdbc_url '{jdbc_url!s}' \
          --result_file /home/hadoop/test_results.txt
  '''.format(
      luigi_module=args.task_module,
      luigi_task=args.task,
      jdbc_url=jdbc_url
  ))


def download_and_unpack_release(cluster, args):
  path = args.lynx_release_dir
  if path:
    cluster.rsync_up(path + '/', '/mnt/lynx')
  else:
    version = args.lynx_version
    cluster.ssh('''
      set -x
      cd /mnt
      if [ ! -f "./lynx-{version!s}.tgz" ]; then
        ./download-lynx-{version!s}.sh
        mkdir -p lynx
        tar xfz lynx-{version!s}.tgz -C lynx --strip-components 1
      fi
      '''.format(version=version))


def install_docker_and_lynx(cluster, version):
  cluster.ssh('''
    set -x
    cd /mnt/lynx
    if ! hash docker 2>/dev/null; then
      echo "docker not found, installing"
      sudo LYNXKITE_USER=hadoop ./install-docker.sh
    fi
    # no need for scheduled tasks today
    echo "sleep_period_seconds: 1000000000\nscheduling: []" >lynx/luigi_tasks/chronomaster.yml
    # Log out here to refresh docker user group.
  '''.format(version=version))


def start_or_reset_ecosystem_docker(cluster, version):
  kite_config = '''
    KITE_INSTANCE: ecosystem-test
    KITE_DATA_DIR: hdfs://\$HOSTNAME:8020/user/\$USER/lynxkite_data/
    KITE_MASTER_MEMORY_MB: 8000
    NUM_EXECUTORS: {num_executors!s}
    EXECUTOR_MEMORY: 18g
    NUM_CORES_PER_EXECUTOR: 8
'''.format(num_executors=args.emr_instance_count - 1)

  res = cluster.ssh('''
    cd /mnt/lynx/lynx
    # Start ecosystem.
    if [[ $(docker ps -qf name=lynx_luigi_worker_1) ]]; then
      echo "Ecosystem is running. Cleanup and Luigi restart is needed."
      hadoop fs -rm -r /user/hadoop/lynxkite_data
      sudo rm -Rf ./lynxkite_metadata/*
      ./reload_luigi_tasks.sh
    else
      # Update configuration:
      sed -i.bak '/Please configure/q' docker-compose-lynxkite.yml
      cat >>docker-compose-lynxkite.yml <<EOF{kite_config!s}EOF
      ./start.sh
    fi
    # Wait for ecosystem startup completion.
    # (We keep retrying a dummy task until it succeeds.)
    docker exec lynx_luigi_worker_1 rm -f /tasks_data/smoke_test_marker.txt
    docker exec lynx_luigi_worker_1 rm -Rf /tmp/luigi/
    while [[ $(docker exec lynx_luigi_worker_1 cat /tasks_data/smoke_test_marker.txt 2>/dev/null) != "done" ]]; do
      docker exec lynx_luigi_worker_1 luigi --module test_tasks.smoke_test SmokeTest
      sleep 1
    done'''.format(
      version=args.lynx_version,
      kite_config=kite_config))


def start_tests_docker(cluster, jdbc_url):
  '''Start running the tests in the background.'''
  cluster.ssh_nohup('''
      docker exec lynx_luigi_worker_1 python3 tasks/test_tasks/test_runner.py \
          --module {luigi_module!s} \
          --task {luigi_task!s} \
          --task-param jdbc_url '{jdbc_url!s}' \
          --result_file /tmp/test_results.txt
      docker exec lynx_luigi_worker_1 cat /tmp/test_results.txt >/home/hadoop/test_results.txt
  '''.format(
      luigi_module=args.task_module,
      luigi_task=args.task,
      jdbc_url=jdbc_url
  ))


def prompt_delete():
  if args.rm:
    return True
  print('Terminate instances? [y/N] ', end='')
  choice = input().lower()
  if choice == 'y':
    return True
  else:
    print('''Please don't forget to terminate the instances!''')
    return False


def shut_down_instances(cluster, db):
  if prompt_delete():
    print('Shutting down instances.')
    cluster.terminate()
    db.terminate()


if __name__ == '__main__':
  args = parser.parse_args()
  main(args)
