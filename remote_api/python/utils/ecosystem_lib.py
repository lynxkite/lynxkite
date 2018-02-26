from utils.emr_lib import EMRLib
import os
import argparse
import datetime
import sys

arg_parser = argparse.ArgumentParser()


arg_parser.add_argument(
    '--cluster_name',
    default=os.environ['USER'] + '-ecosystem-test',
    help='Name of the cluster to start')
arg_parser.add_argument(
    '--public_ip',
    default=None,
    help='The Elastic IP associated to the cluster')
arg_parser.add_argument(
    '--ec2_key_file',
    default=os.environ['HOME'] + '/.ssh/lynx-cli.pem')
arg_parser.add_argument(
    '--ec2_key_name',
    default='lynx-cli')
arg_parser.add_argument(
    '--emr_region',
    default='us-east-1',
    help='Region of the EMR cluster.' +
    ' Possible values: us-east-1, ap-southeast-1, eu-central-1, ...')
arg_parser.add_argument(
    '--emr_log_uri',
    default='s3://test-ecosystem-log',
    help='URI of the S3 bucket where the EMR logs will be written.')
arg_parser.add_argument(
    '--no_emr_log',
    action='store_true',
    help='If this is set, no EMR logs will be written to S3 (regardless of --emr_log_uri)'
)
arg_parser.add_argument(
    '--with_rds',
    action='store_true',
    help='Spin up a mysql RDS instance to test database operations.')
arg_parser.add_argument(
    '--with_jupyter',
    action='store_true',
    help='''If it is set, a jupyter notebook server will be installed and started
  on the cluster. Some Python packages (sklearn, matplotlib) will also be added.''')
arg_parser.add_argument(
    '--biggraph_releases_dir',
    default=os.environ['HOME'] + '/biggraph_releases',
    help='''Directory containing the downloader script, typically the root of
         the biggraph_releases repo. The downloader script will have the form of
         BIGGRAPH_RELEASES_DIR/download-lynx-LYNX_VERSION.sh''')
arg_parser.add_argument(
    '--lynx_version',
    default='',
    help='''Version of the ecosystem release to test. A downloader script of the
          following form will be used for obtaining the release:
         BIGGRAPH_RELEASES_DIR/download-lynx-LYNX_VERSION.sh''')
arg_parser.add_argument(
    '--lynx_release_dir',
    default='ecosystem/native/dist',
    help='''If non-empty, then this local directory is directly uploaded instead of
         using LYNX_VERSION and BIGGRAPH_RELEASES_DIR. The directory of the current
         native code is ecosystem/native/dist.''')
arg_parser.add_argument(
    '--log_dir',
    default='',
    help='''Cluster log files are downloaded to this directory.
    If it is an empty string, no log file is downloaded.''')
arg_parser.add_argument(
    '--s3_data_dir',
    help='S3 path to be used as non-ephemeral data directory.')
arg_parser.add_argument(
    '--restore_metadata',
    action='store_true',
    help='''If it is set, metadata will be reloaded from `s3_data_dir/metadata_backup/`,
  after the cluster was started. The metadata will not automatically be
  saved when the cluster is shut down.''')
arg_parser.add_argument(
    '--s3_metadata_version',
    help='''If specified, it defines the VERSION part of the metadata backup directory:
    s3_data_dir/metadata_backup/VERSION The format of this flag (and VERSION) is
    `YYYYMMddHHmmss` e.g. `20170123164600`. If not specified , the script will use the
    latest version in `s3_data_dir/metadata_backup/`.''')
arg_parser.add_argument(
    '--owner',
    default=os.environ['USER'],
    help='''The responsible person for this EMR cluster.''')
arg_parser.add_argument(
    '--kite_instance_name',
    default='ecosystem_test',
    help='This sets the KITE_INSTANCE environment variable for LynxKite')
arg_parser.add_argument(
    '--expiry',
    default=(
        datetime.date.today() + datetime.timedelta(days=1)).strftime("%Y%m%d"),
    help='''The "expiration date" of this cluster in "YYYYmmdd" format.
  After this date the 'owner' will be asked if the cluster can
  be shut down.''')
arg_parser.add_argument(
    '--with_tasks',
    help='If specified, the Luigi tasks are copied to the cluster')
arg_parser.add_argument(
    '--python_dependencies',
    help='Install Python dependencies in the specified file')
arg_parser.add_argument(
    '--applications',
    help='''Applications to start on the cluster like Hive, Hue, Pig... as
  a comma separated list. (e.g. "Hive,Hue").''')
arg_parser.add_argument(
    '--env_variables',
    help='''The environment variables which are to be set to be the same on the master
  machine as on the local machine. Takes a comma separated list.
  (e.g. "INPUT_FOLDER,OUTPUT_FOLDER").''')
arg_parser.add_argument(
    '--upload',
    help='''Files or folders to be upload after all the usual uploads are done but before LynxKite
  is started. Can be used to override config files (e.g. prefix_definitions.txt) for testing. Takes
  a comma separated list of source:destination pairs.
  E.g.: ${TEST_CONFIGS}/prefix_definitions.txt:/mnt/lynx/config/prefix_definitions.txt,...''')
arg_parser.add_argument(
    '--master_instance_type',
    help='''The type of the instance that will run the LynxKite driver. (E.g., m3.xlarge)''',
    default='m3.2xlarge')
arg_parser.add_argument(
    '--core_instance_type',
    help='''The type of the instance that will run the executors. (E.g., m3.xlarge)''',
    default='m3.2xlarge')
arg_parser.add_argument(
    '--emr_instance_count',
    type=int,
    default=3,
    help='Number of instances on EMR cluster, including master.')
arg_parser.add_argument(
    '--spot',
    action='store_true',
    help='Use spot instances instead of on demand instances.')
arg_parser.add_argument(
    '--spot_bid_multiplier',
    default=1.0,
    type=float,
    help='''Set the spot bid price for the instances to SPOT_BID_MULTIPLIER times the on demand price.
    The default is 1.0, as recommended by AWS, but for short-lived, important instances you
    can raise this quantity to reduce the possibility of losing your work. (E.g., setting this to
    3.0 means that the instances will not be terminated until the spot price rises above
    three times the on demand price.''')
arg_parser.add_argument(
    '--autoscaling_role',
    action='store_true',
    help='Use this option to enable Auto Scaling.')
arg_parser.add_argument(
    '--master_ebs_volume_size',
    type=int,
    default=0,
    help='Size of EBS volume (In GB) to be attached to the Master instance.')
arg_parser.add_argument(
    '--core_ebs_volume_size',
    type=int,
    default=0,
    help='Size of EBS volume (In GB) to be attached to the Core instance.')
arg_parser.add_argument(
    '--kite_master_memory_mb',
    default=8000,
    help='Set KITE_MASTER_MEMORY_MB in kiterc'
)
arg_parser.add_argument(
    '--executor_memory',
    default='18g',
    help='Set EXECUTOR_MEMORY in kiterc'
)
arg_parser.add_argument(
    '--num_cores_per_executor',
    default=8,
    help='Set NUM_CORES_PER_EXECUTOR in kiterc'
)


class Ecosystem:

  def __init__(self, args):
    log_uri = None if args.no_emr_log else args.emr_log_uri
    self.cluster_config = {
        'cluster_name': args.cluster_name,
        'public_ip': args.public_ip,
        'ec2_key_file': args.ec2_key_file,
        'ec2_key_name': args.ec2_key_name,
        'emr_region': args.emr_region,
        'emr_instance_count': args.emr_instance_count,
        'emr_log_uri': log_uri,
        'hdfs_replication': '1',
        'with_rds': args.with_rds,
        'with_jupyter': args.with_jupyter,
        'rm': args.rm,
        'spot': args.spot,
        'spot_bid_multiplier': args.spot_bid_multiplier,
        'autoscaling_role': args.autoscaling_role,
        'owner': args.owner,
        'expiry': args.expiry,
        'applications': args.applications,
        'master_instance_type': args.master_instance_type,
        'master_ebs_volume_size': args.master_ebs_volume_size,
        'core_instance_type': args.core_instance_type,
        'core_ebs_volume_size': args.core_ebs_volume_size,
    }
    self.lynxkite_config = {
        'biggraph_releases_dir': args.biggraph_releases_dir,
        'lynx_version': args.lynx_version,
        'lynx_release_dir': args.lynx_release_dir,
        'log_dir': args.log_dir,
        'kite_instance_name': args.kite_instance_name,
        's3_data_dir': args.s3_data_dir,
        'restore_metadata': args.restore_metadata,
        's3_metadata_version': args.s3_metadata_version,
        's3_metadata_dir': '',
        'tasks': args.with_tasks,
        'extra_python_dependencies': args.python_dependencies,
        'env_variables': args.env_variables,
        'upload': args.upload,
        'num_cores_per_executor': args.num_cores_per_executor,
        'executor_memory': args.executor_memory,
        'kite_master_memory_mb': args.kite_master_memory_mb,
    }
    self.cluster = None
    self.instances = []
    self.jdbc_url = ''
    self.s3_client = None

  def launch_cluster(self):
    print('Launching an EMR cluster.')
    # Create an EMR cluster.
    conf = self.cluster_config
    lib = EMRLib(
        ec2_key_file=conf['ec2_key_file'],
        ec2_key_name=conf['ec2_key_name'],
        region=conf['emr_region'])
    self.s3_client = lib.s3_client
    self.cluster = lib.create_or_connect_to_emr_cluster(
        name=conf['cluster_name'],
        log_uri=conf['emr_log_uri'],
        owner=conf['owner'],
        expiry=conf['expiry'],
        instance_count=conf['emr_instance_count'],
        hdfs_replication=conf['hdfs_replication'],
        applications=conf['applications'],
        core_instance_type=conf['core_instance_type'],
        master_instance_type=conf['master_instance_type'],
        spot=conf['spot'],
        spot_bid_multiplier=conf['spot_bid_multiplier'],
        autoscaling_role=conf['autoscaling_role'],
        master_ebs_volume_size=conf['master_ebs_volume_size'],
        core_ebs_volume_size=conf['core_ebs_volume_size']
    )
    self.instances = [self.cluster]
    # Spin up a mysql RDS instance only if requested.
    if conf['with_rds']:
      mysql_instance = lib.create_or_connect_to_rds_instance(
          name=conf['cluster_name'] + '-mysql')
      # Wait for startup of both.
      self.instances = self.instances + [mysql_instance]
      lib.wait_for_services(self.instances)
      mysql_address = mysql_instance.get_address()
      self.jdbc_url = 'jdbc:mysql://{mysql_address}/db?user=root&password=rootroot'.format(
          mysql_address=mysql_address)
    else:
      lib.wait_for_services(self.instances)
    if conf['public_ip']:
      self.cluster.associate_address(conf['public_ip'])

  def start(self):
    print('Starting LynxKite on EMR cluster.')
    conf = self.cluster_config
    lk_conf = self.lynxkite_config
    self.upload_test_tasks()
    if lk_conf['extra_python_dependencies']:
      self.install_extra_python_dependencies(lk_conf['extra_python_dependencies'])
    self.upload_tools()
    self.install_lynx_stuff(
        lk_conf['lynx_release_dir'],
        lk_conf['lynx_version'],
        lk_conf['biggraph_releases_dir'])
    self.install_native_dependencies()
    self.set_s3_metadata_dir(
        lk_conf['s3_data_dir'],
        lk_conf['s3_metadata_version'])
    self.config_and_prepare_native(
        lk_conf['s3_data_dir'],
        lk_conf['kite_instance_name'],
        conf['emr_instance_count'],
        lk_conf['num_cores_per_executor'],
        lk_conf['executor_memory'],
        lk_conf['kite_master_memory_mb'])
    self.config_aws_s3_native()
    if conf['with_jupyter']:
      self.install_and_setup_jupyter()
    if conf['applications'] and 'hive' in [a.lower() for a in conf['applications'].split(',')]:
      self.hive_patch()
    self.start_monitoring_on_extra_nodes_native(conf['ec2_key_file'])
    if lk_conf['tasks']:
      self.upload_tasks(src=lk_conf['tasks'])
    if lk_conf['env_variables']:
      variables = lk_conf['env_variables'].split(',')
      self.copy_environment_variables(variables)
    if lk_conf['upload']:
      split = lk_conf['upload'].split(',')
      uploads_map = {i.split(':')[0]: i.split(':')[1] for i in split}
      self.upload_specified(uploads_map)
    self.start_supervisor_native()
    print('LynxKite ecosystem was started by supervisor.')

  def set_s3_metadata_dir(self, s3_bucket, metadata_version):
    if s3_bucket and self.lynxkite_config['restore_metadata']:
      # s3_bucket = 's3://bla/, bucket = 'bla'
      bucket = s3_bucket.split('/')[2]
      if not metadata_version:
        # Gives back the "latest" version from the `s3_bucket/metadata_backup/`.
        # http://boto3.readthedocs.io/en/latest/reference/services/s3.html#examples
        paginator = self.s3_client.get_paginator('list_objects')
        result = paginator.paginate(Bucket=bucket, Prefix='metadata_backup/', Delimiter='/')
        # Name of the alphabetically last folder without the trailing slash.
        # If 'metadata_backup' is missing or empty, the following line throws an exception.
        version = sorted([prefix.get('Prefix')
                          for prefix in result.search('CommonPrefixes')])[-1][:-1]
      else:
        version = metadata_version
      self.lynxkite_config['s3_metadata_dir'] = 's3://{buc}/{ver}/'.format(
          buc=bucket,
          ver=version)

  def restore_metadata(self):
    s3_metadata_dir = self.lynxkite_config['s3_metadata_dir']
    print('Restoring metadata from {dir}...'.format(dir=s3_metadata_dir))
    self.cluster.ssh('''
      set -x
      cd /mnt/lynx
      supervisorctl stop lynxkite
      if [ -d metadata/lynxkite ]; then
        mv metadata/lynxkite metadata/lynxkite.$(date "+%Y%m%d_%H%M%S_%3N")
      fi
      aws s3 sync {dir} metadata/lynxkite/ --exclude "*$folder$" --quiet
      supervisorctl start lynxkite
    '''.format(dir=s3_metadata_dir))
    print('Metadata restored.')

  def run_tests(self, test_config):
    conf = self.cluster_config
    print('Running tests on EMR cluster.')
    self.start_tests_native(
        self.jdbc_url,
        test_config['task_module'],
        test_config['task'],
        test_config['dataset'])
    print('Tests are now running in the background. Waiting for results.')
    self.cluster.fetch_output()
    res_dir = test_config['results_local_dir']
    res_name = test_config['results_name']
    if not os.path.exists(res_dir):
      os.makedirs(res_dir)
    self.cluster.rsync_down(
        '/home/hadoop/test_results.txt',
        res_dir + res_name)
    self.upload_perf_logs_to_gcloud(conf['cluster_name'])

  def cleanup(self):
    lk_conf = self.lynxkite_config
    print('Downloading logs and stopping EMR cluster.')
    if lk_conf['log_dir']:
      self.download_logs_native(lk_conf['log_dir'])
    self.shut_down_instances()

  ###
  ### ecosystem launch methods ###
  ###

  def upload_installer_script(self, releases_dir, lynx_version):
    self.cluster.rsync_up(
        src='{dir}/download-lynx-{version}.sh'.format(
            dir=releases_dir,
            version=lynx_version),
        dst='/mnt/')

  def install_extra_python_dependencies(self, requirements):
    self.cluster.rsync_up(requirements, '.')
    file_name = os.path.basename(requirements)
    self.cluster.ssh('sudo pip-3.6 install --upgrade -r {}'.format(file_name))

  def upload_tasks(self, src, dst='/mnt/lynx/luigi_tasks/'):
    self.cluster.ssh('mkdir -p ' + dst)
    self.cluster.rsync_up(src, dst)

  def upload_test_tasks(self):
    self.upload_tasks('ecosystem/tests/', '/mnt/lynx/luigi_tasks/test_tasks')
    self.cluster.ssh('''
        set -x
        cd /mnt/lynx/luigi_tasks/test_tasks
        mv test_runner.py /mnt/lynx/luigi_tasks
        ''')

  def upload_tools(self):
    target_dir = '/mnt/lynx/tools'
    self.cluster.ssh('mkdir -p ' + target_dir)
    self.cluster.rsync_up('ecosystem/native/tools/', target_dir)
    self.cluster.rsync_up('tools/performance_collection/', target_dir)

  def upload_specified(self, uploads_map):
    for src, dst in uploads_map.items():
      self.cluster.rsync_up(src, dst)

  def install_lynx_stuff(self, lynx_release_dir, lynx_version, releases_dir):
    if lynx_version:
      self.cluster.rsync_up(
          src='{dir}/download-lynx-{version}.sh'.format(
              dir=releases_dir,
              version=lynx_version),
          dst='/mnt/')
      self.cluster.ssh('''
        set -x
        cd /mnt
        if [ ! -f "./lynx-{version}.tgz" ]; then
          ./download-lynx-{version}.sh
          mkdir -p lynx
          tar xfz lynx-{version}.tgz -C lynx --strip-components 1
        fi
        '''.format(version=lynx_version))
    else:
      self.cluster.rsync_up(lynx_release_dir + '/', '/mnt/lynx')

  def install_native_dependencies(self):
    self.cluster.rsync_up('python_requirements.txt', '/mnt/lynx')
    self.cluster.ssh('''
    set -x
    cd /mnt/lynx
    sudo yum install -y python36 python36-devel python36-pip mysql-server gcc libffi-devel
    # Removes the given and following lines so only the necessary modules will be installed.
    sed -i -n '/# Dependencies for developing and testing/q;p'  python_requirements.txt
    sudo pip-3.6 install --upgrade -r python_requirements.txt
    sudo pip-2.7 install --upgrade setuptools
    sudo pip-2.7 install --upgrade requests[security] supervisor
    # mysql setup
    sudo service mysqld start
    mysqladmin  -u root password 'root' || true  # (May be set already.)
    # This mysql database is used for many things, including the testing of JDBC tasks.
    # For that purpose access needs to be granted for all executors.
    mysql -uroot -proot -e "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'root'"
    ''')

  def config_and_prepare_native(self, s3_data_dir,
                                kite_instance_name,
                                emr_instance_count,
                                num_cores_per_executor,
                                executor_memory,
                                kite_master_memory_mb):
    hdfs_path = 'hdfs://$HOSTNAME:8020/user/$USER/lynxkite/'
    if s3_data_dir:
      data_dir_config = '''
        export KITE_DATA_DIR={}
        export KITE_EPHEMERAL_DATA_DIR={}
      '''.format(s3_data_dir, hdfs_path)
    else:
      data_dir_config = '''
        export KITE_DATA_DIR={}
      '''.format(hdfs_path)
    progname = os.path.basename(sys.argv[0])
    self.cluster.ssh('''
      cd /mnt/lynx
      echo 'Setting up environment variables.'
      # Set HADOOP_CONF_DIR before anything else.
      if ! head -n1 config/central | grep -q "export HADOOP_CONF_DIR=/etc/hadoop/conf"
      then
        sed -i '1s;^;export HADOOP_CONF_DIR=/etc/hadoop/conf\\n;' config/central
      fi
      # Removes the given and following lines so config/central does not grow constantly.
      sed -i -n '/# ---- the below lines were added by {progname} ----/q;p'  config/central
      cat >>config/central <<'EOF'
# ---- the below lines were added by {progname} ----
        export KITE_INSTANCE={kite_instance_name}
        export KITE_MASTER_MEMORY_MB={kite_master_memory_mb}
        export NUM_EXECUTORS={num_executors}
        export EXECUTOR_MEMORY={executor_memory}
        export NUM_CORES_PER_EXECUTOR={num_cores_per_executor}
        # port differs from the one used in central/config
        export HDFS_ROOT=hdfs://$HOSTNAME:8020/user/$USER
        {data_dir_config}
        export LYNXKITE_ADDRESS=https://localhost:$KITE_HTTPS_PORT/
        export PYTHONPATH=/mnt/lynx/apps/remote_api/python/:/mnt/lynx/luigi_tasks
        export LYNX=/mnt/lynx
        # for tests with mysql server on master
        export DATA_DB=jdbc:mysql://$HOSTNAME:3306/'db?user=root&password=root&rewriteBatchedStatements=true'
        export KITE_INTERNAL_WATCHDOG_TIMEOUT_SECONDS=7200
EOF
      echo 'Creating hdfs directory.'
      source config/central
      hdfs dfs -mkdir -p $KITE_DATA_DIR/table_files
      echo 'Creating tasks_data directory.'
      # TODO: Find a more sane directory.
      sudo mkdir -p /tasks_data
      sudo chmod a+rwx /tasks_data
    '''.format(
        num_cores_per_executor=num_cores_per_executor,
        kite_master_memory_mb=kite_master_memory_mb,
        executor_memory=executor_memory,
        progname=progname,
        kite_instance_name=kite_instance_name,
        num_executors=emr_instance_count - 1,
        data_dir_config=data_dir_config))

  def config_aws_s3_native(self):
    self.cluster.ssh('''
      cd /mnt/lynx
      echo 'Setting s3 prefix.'
      mv config/prefix_definitions.txt config/prefix_definitions.bak
      cat >config/prefix_definitions.txt <<'EOF'
S3="s3://"
EOF
      echo 'Setting AWS CLASSPATH.'
      cat >spark/conf/spark-env.sh <<'EOF'
AWS_CLASSPATH1=$(find /usr/share/aws/emr/emrfs/lib -name "*.jar" | tr '\\n' ':')
AWS_CLASSPATH2=$(find /usr/share/aws/aws-java-sdk -name "*.jar" | tr '\\n' ':')
AWS_CLASSPATH3=$(find /usr/share/aws/emr/instance-controller/lib -name "*.jar" | tr '\\n' ':')
AWS_CLASSPATH_ALL=$AWS_CLASSPATH1$AWS_CLASSPATH2$AWS_CLASSPATH3
export SPARK_DIST_CLASSPATH=$SPARK_DIST_CLASSPATH:${AWS_CLASSPATH_ALL::-1}
EOF
      chmod a+x spark/conf/spark-env.sh
    ''')
    #  This is needed because of a Spark 2 - EMR - YARN - jersey conflict
    #  Disables timeline service in yarn.
    #  https://issues.apache.org/jira/browse/SPARK-15343
    self.cluster.ssh('''
      cd /mnt/lynx/spark/conf
      cat >spark-defaults.conf <<'EOF'
spark.hadoop.yarn.timeline-service.enabled false
EOF
    ''')

  def hive_patch(self):
    # Configure Hive with Spark:
    # execution engine = mr
    self.cluster.ssh('''
      cp /etc/hive/conf/hive-site.xml /mnt/lynx/spark/conf/
      cd /mnt/lynx/spark/conf/
      sed -i -e 's#<value>tez</value>#<value>mr</value>#g' hive-site.xml
    ''')

  def set_environment_variables(self, variables_dict):
    setting_variables_list = ['export {variable}={value}'.format(
        variable=self.safe_value(variable), value=self.safe_value(value))
        for variable, value in variables_dict.items()]
    setting_variables_string = '\n'.join([''] + setting_variables_list)
    self.cluster.ssh('echo "{}" >> ~/.bashrc'.format(setting_variables_string))

  def safe_value(self, value):
    from shlex import quote
    # quote can not seem to handle None value.
    return quote(value) if value else ''

  def copy_environment_variables(self, variables):
    variables_dict = {var: os.environ.get(var) for var in variables}
    self.set_environment_variables(variables_dict)

  def start_monitoring_on_extra_nodes_native(self, keyfile):
    cluster_keyfile = 'cluster_key.pem'
    cluster_keypath = '/home/hadoop/.ssh/' + cluster_keyfile
    self.cluster.rsync_up(src=keyfile, dst=cluster_keypath)
    ssh_options = '''-o UserKnownHostsFile=/dev/null \
      -o CheckHostIP=no \
      -o StrictHostKeyChecking=no \
      -i /home/hadoop/.ssh/{keyfile}'''.format(keyfile=cluster_keyfile)

    self.cluster.ssh('''
      yarn node -list -all | grep RUNNING | cut -d':' -f 1 > nodes.txt
      ''')

    self.cluster.ssh('''
      for node in `cat nodes.txt`; do
        scp {options} \
          /mnt/lynx/other_nodes/other_nodes.tgz \
          hadoop@${{node}}:/home/hadoop/other_nodes.tgz
        ssh {options} hadoop@${{node}} tar xf other_nodes.tgz
        ssh {options} hadoop@${{node}} "sh -c 'nohup ./run.sh >run.stdout 2> run.stderr &'"
      done'''.format(options=ssh_options))

    # We no longer need the key file on the master
    self.cluster.ssh('shred -u {key}'.format(key=cluster_keypath))

    # Uncomment services in configs
    self.cluster.ssh('''
      /mnt/lynx/tools/uncomment_config.sh /mnt/lynx/config/monitoring/prometheus.yml
      /mnt/lynx/tools/uncomment_config.sh /mnt/lynx/config/supervisord.conf
      ''')

    self.cluster.ssh('''
      /mnt/lynx/scripts/service_explorer.sh
    ''')

  def start_supervisor_native(self):
    self.cluster.ssh_nohup('''
      set -x
      source /mnt/lynx/config/central
      /usr/local/bin/supervisord -c config/supervisord.conf
      ''')

  def install_and_setup_jupyter(self):
    self.cluster.ssh('''
      sudo pip-3.4 install --upgrade jupyter sklearn matplotlib
      sudo pip-3.4 install --upgrade pandas seaborn statsmodels
    ''')
    self.cluster.ssh_nohup('''
      mkdir -p /mnt/lynx/notebooks
      source /mnt/lynx/config/central
      cd /mnt/lynx/notebooks
      jupyter-notebook --NotebookApp.token='' --port=2202
      ''')

  ###
  ### test runner methods ###
  ###

  def start_tests_native(self, jdbc_url, task_module, task, dataset):
    '''Start running the tests in the background.'''
    self.cluster.ssh_nohup('''
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
        luigi_module=task_module,
        luigi_task=task,
        jdbc_url=jdbc_url,
        dataset=dataset,
    ))

  def upload_perf_logs_to_gcloud(self, cluster_name):
    print('Uploading performance logs to gcloud.')
    instance_name = 'emr-' + cluster_name
    self.cluster.ssh('''
      cd /mnt/lynx
      tools/multi_upload.sh 0 apps/lynxkite/logs {i}
    '''.format(i=instance_name))

  ###
  ### cleanup methods ###
  ###

  def download_logs_native(self, log_dir):
    if not os.path.exists(log_dir):
      os.makedirs(log_dir)
    self.cluster.rsync_down('/mnt/lynx/logs/', log_dir)
    self.cluster.rsync_down('/mnt/lynx/apps/lynxkite/logs/', log_dir + '/lynxkite-logs/')

  def shut_down_instances(self):
    if self.prompt_delete():
      print('Shutting down instances.')
      self.cluster.turn_termination_protection_off()
      for instance in self.instances:
        instance.terminate()

  def prompt_delete(self):
    if self.cluster_config['rm']:
      return True
    print('Terminate instances? [y/N] ', end='')
    choice = input().lower()
    if choice == 'y':
      return True
    else:
      print('''Please don't forget to terminate the instances!''')
      return False
