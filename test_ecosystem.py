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


#  Big data test sets in the  `s3://lynxkite-test-data/` bucket.
#  fake_westeros_v3_100k_2m     100k vertices, 2m edges (small)
#  fake_westeros_v3_5m_145m     5m vertices, 145m edges (normal)
#  fake_westeros_v3_10m_303m    10m vertices, 303m edges (large)
#  fake_westeros_v3_25m_799m    25m vertices 799m edges (xlarge)

test_sets = {
    'small': 'fake_westeros_v3_100k_2m',
    'normal': 'fake_westeros_v3_5m_145m',
    'large': 'fake_westeros_v3_10m_303m',
    'xlarge': 'fake_westeros_v3_25m_799m'
}


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
    default='native-1.9.5',
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
    '--ec2_key_file',
    default=os.environ['HOME'] + '/.ssh/lynx-cli.pem')
parser.add_argument(
    '--ec2_key_name',
    default='lynx-cli')
parser.add_argument(
    '--emr_instance_count',
    type=int,
    default=3,
    help='Number of instances on EMR cluster, including master.')
parser.add_argument(
    '--results_dir',
    default='./ecosystem/tests/results/')
parser.add_argument(
    '--rm',
    action='store_true',
    help='''Delete the cluster after completion.''')
parser.add_argument(
    '--dockerized',
    action='store_true',
    help='Start the docker version. Without this switch the default is native.')
parser.add_argument(
    '--monitor_nodes',
    action='store_true',
    help='Setup and start monitoring on the extra nodes. The default is false.')
parser.add_argument(
    '--bigdata',
    action='store_true',
    help='The given task is a big data test task. A bigdata_test_set parameter also have to be given.')
parser.add_argument(
    '--bigdata_test_set',
    default='small',
    help='Test set for big data tests. Possible values: small, normal, large, xlarge.')
parser.add_argument(
    '--emr_log_uri',
    default='s3://test-ecosystem-log',
    help='URI of the S3 bucket where the EMR logs will be written.'
)


def main(args):
  # Checking argument dependencies.
  check_arguments(args)
  # Create an EMR cluster and a MySQL database in RDS.
  lib = EMRLib(
      ec2_key_file=args.ec2_key_file,
      ec2_key_name=args.ec2_key_name)
  cluster = lib.create_or_connect_to_emr_cluster(
      name=args.cluster_name,
      log_uri=args.emr_log_uri,
      instance_count=args.emr_instance_count
  )
  mysql_instance = lib.create_or_connect_to_rds_instance(
      name=args.cluster_name + '-mysql')
  # Wait for startup of both.
  lib.wait_for_services([cluster, mysql_instance])

  mysql_address = mysql_instance.get_address()
  jdbc_url = 'jdbc:mysql://{mysql_address}/db?user=root&password=rootroot'.format(
      mysql_address=mysql_address)

  upload_installer_script(cluster, args)
  upload_tasks(cluster, args)
  upload_tools(cluster, args)
  download_and_unpack_release(cluster, args)
  if args.dockerized:
    install_docker_and_lynx(cluster, args.lynx_version)
    # TODO: config_aws_s3_docker(cluster)
    start_or_reset_ecosystem_docker(cluster, args.lynx_version)
    start_tests_docker(cluster, jdbc_url, args)
  else:
    install_native(cluster)
    config_and_prepare_native(cluster, args)
    config_aws_s3_native(cluster)
    if args.monitor_nodes:
      start_monitoring_on_extra_nodes_native(args.ec2_key_file, cluster)
    start_supervisor_native(cluster)
    start_tests_native(cluster, jdbc_url, args)
  print('Tests are now running in the background. Waiting for results.')
  cluster.fetch_output()
  if not os.path.exists(results_local_dir(args)):
    os.makedirs(results_local_dir(args))
  cluster.rsync_down('/home/hadoop/test_results.txt', results_local_dir(args) + results_name(args))
  shut_down_instances(cluster, mysql_instance)


def results_local_dir(args):
  '''
  In case of big data tests, the name of the result dir includes the number of instances,
  the number of executors and the name of the test data set.
  '''
  if args.bigdata:
    basedir = args.results_dir
    dataset = bigdata_test_set(args.bigdata_test_set)
    instance_count = args.emr_instance_count
    executors = instance_count - 1
    return "{bd}emr_{e}_{i}_{ds}".format(
        bd=basedir,
        e=executors,
        i=instance_count,
        ds=dataset
    )
  else:
    return args.results_dir


def results_name(args):
  return "/{task}-result.txt".format(
      task=args.task
  )


def check_docker_vs_native(args):
  '''
  Try to check if the given release is a docker release if and only if the `--dockerized` switch is used.
  '''
  if args.dockerized:
    if args.monitor_nodes:
      raise ValueError('Dockerized version does not support monitor_nodes')
    if args.lynx_release_dir:
      if 'native' in args.lynx_release_dir:
        raise ValueError('You cannot use a native release dir to test a dockerized version.')
    else:
      if 'native' in args.lynx_version:
        raise ValueError('You cannot use a native release to test a dockerized version.')
  else:
    if args.lynx_release_dir:
      if 'native' not in args.lynx_release_dir:
        raise ValueError('You need a native release dir to test the native ecosystem.')
    else:
      if 'native' not in args.lynx_version:
        raise ValueError('You need a native release to test the native ecosystem.')


def check_bigdata(args):
  '''Possible values of `--bigdata_test_set`.'''
  if args.bigdata:
    if args.bigdata_test_set not in test_sets.keys():
      raise ValueError('Parameter = '
                       + args.bigdata_test_set
                       + ', possible values are: '
                       + ", ".join(test_sets.keys()))


def check_arguments(args):
  check_docker_vs_native(args)
  check_bigdata(args)


def bigdata_test_set(test_set):
  return test_sets[test_set]


def upload_installer_script(cluster, args):
  if not args.lynx_release_dir:
    cluster.rsync_up(
        src='{dir}/download-lynx-{version}.sh'.format(
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


def upload_tools(cluster, args):
  if not args.dockerized:
    target_dir = '/mnt/lynx/tools'
    cluster.ssh('mkdir -p ' + target_dir)
    cluster.rsync_up('ecosystem/native/tools/', target_dir)


def install_native(cluster):
  cluster.ssh('''
    set -x
    cd /mnt/lynx
    sudo yum install -y python34-pip mysql-server gcc libffi-devel
    sudo pip-3.4 install --upgrade luigi sqlalchemy mysqlclient PyYAML prometheus_client
    # Temporary workaround needed because of the pycparser 2.14 bug.
    sudo pip-2.6 install pycparser==2.13
    sudo pip-2.6 install cryptography
    sudo pip-2.6 install --upgrade requests[security] supervisor
    # mysql setup
    sudo service mysqld start
    mysqladmin  -u root password 'root'
    # This mysql database is used for many things, including the testing of JDBC tasks.
    # For that purpose access needs to be granted for all executors.
    mysql -uroot -proot -e "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'root'"
  ''')


def config_and_prepare_native(cluster, args):
  cluster.ssh('''
    cd /mnt/lynx
    echo 'Setting up environment variables.'
    # Removes the given and following lines.
    sed -i -n '/# ---- the below lines were added by test_ecosystem.py ----/q;p'  config/central
    cat >>config/central <<'EOF'
# ---- the below lines were added by test_ecosystem.py ----
      export KITE_INSTANCE=ecosystem-test
      export KITE_MASTER_MEMORY_MB=8000
      export NUM_EXECUTORS={num_executors}
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
    sed -i -n '/# ---- the below lines were added by test_ecosystem.py ----/q;p'  config/prefix_definitions.txt
    cat >>config/prefix_definitions.txt <<'EOF'
# ---- the below lines were added by test_ecosystem.py ----
S3="s3://"
EOF
    echo 'Setting AWS CLASSPATH.'
    sed -i -n '/# ---- the below lines were added by test_ecosystem.py ----/q;p'  spark/conf/spark-env.sh
    cat >>spark/conf/spark-env.sh <<'EOF'
# ---- the below lines were added by test_ecosystem.py ----
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


def start_monitoring_on_extra_nodes_native(keyfile, cluster):
  cluster_keyfile = 'cluster_key.pem'
  cluster.rsync_up(src=keyfile, dst='/home/hadoop/.ssh/' + cluster_keyfile)
  ssh_options = '''-o UserKnownHostsFile=/dev/null \
    -o CheckHostIP=no \
    -o StrictHostKeyChecking=no \
    -i /home/hadoop/.ssh/{keyfile}'''.format(keyfile=cluster_keyfile)

  cluster.ssh('''
    yarn node -list -all | grep RUNNING | cut -d':' -f 1 > nodes.txt
    ''')

  cluster.ssh('''
    for node in `cat nodes.txt`; do
      scp {options} \
        /mnt/lynx/other_nodes/other_nodes.tgz \
        hadoop@${{node}}:/home/hadoop/other_nodes.tgz
      ssh {options} hadoop@${{node}} tar xf other_nodes.tgz
      ssh {options} hadoop@${{node}} "sh -c 'nohup ./run.sh >run.stdout 2> run.stderr &'"
    done'''.format(options=ssh_options))

# Uncomment services in configs
  cluster.ssh('''
    /mnt/lynx/tools/uncomment_config.sh /mnt/lynx/config/monitoring/prometheus.yml
    /mnt/lynx/tools/uncomment_config.sh /mnt/lynx/config/supervisord.conf
    ''')

  cluster.ssh('''
    /mnt/lynx/scripts/service_explorer.sh
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
      JDBC_URL='{jdbc_url}' DATASET={dataset} \
      python3 /mnt/lynx/luigi_tasks/test_runner.py \
          --module {luigi_module} \
          --task {luigi_task} \
          --result_file /home/hadoop/test_results.txt
  '''.format(
      luigi_module=args.task_module,
      luigi_task=args.task,
      jdbc_url=jdbc_url,
      dataset=bigdata_test_set(args.bigdata_test_set)
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
      if [ ! -f "./lynx-{version}.tgz" ]; then
        ./download-lynx-{version}.sh
        mkdir -p lynx
        tar xfz lynx-{version}.tgz -C lynx --strip-components 1
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
    NUM_EXECUTORS: {num_executors}
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
      cat >>docker-compose-lynxkite.yml <<EOF{kite_config}EOF
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


def start_tests_docker(cluster, jdbc_url, args):
  '''Start running the tests in the background.'''
  cluster.ssh_nohup('''
      docker exec lynx_luigi_worker_1 \
      JDBC_URL='{jdbc_url}' DATASET={dataset} \
      python3 tasks/test_tasks/test_runner.py \
          --module {luigi_module} \
          --task {luigi_task} \
          --result_file /tmp/test_results.txt
      docker exec lynx_luigi_worker_1 cat /tmp/test_results.txt >/home/hadoop/test_results.txt
  '''.format(
      luigi_module=args.task_module,
      luigi_task=args.task,
      jdbc_url=jdbc_url,
      dataset=bigdata_test_set(args.bigdata_test_set)
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
