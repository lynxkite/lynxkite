'''
Utilities for starting EMR and RDS instances, and manipulating them with
ssh and rsync.
'''

import boto3
import botocore
import subprocess
import sys
import tempfile
import time


def call_cmd(cmd_list, input=None, print_output=True, assert_successful=True):
  '''
  Invoked an OS command with arguments.
  cmd_list: the command and its arguments in a list
  input: A string that is passed to the stding of the invoked
    process.
  print_ouput: Whether to print the output (stdout+stderr) of the
    process to stdout of Python.
  assert_successful: If True, a non-zero exit code is turned into an exception.
  returns: The combined stdout+stderr on success if assert_successful is True. Otherwise a tuple of
    the output and the exit code.
  '''
  proc = subprocess.Popen(
      cmd_list,
      stdin=subprocess.PIPE,
      stderr=subprocess.STDOUT,
      stdout=subprocess.PIPE,
      universal_newlines=True,
      bufsize=0)
  if input:
    proc.stdin.write(input)
  proc.stdin.close()
  result = ''
  # Print stdout of the child process line by line as it is generated.
  while True:
    line = proc.stdout.readline()
    result += line
    if print_output:
      print(line, end='')
    if not line:
      break
  while proc.poll() is None:
    time.sleep(0.1)
  if assert_successful:
    assert proc.returncode == 0, (
        '{} has returned non-zero exit code: {}'.format(cmd_list, proc.returncode))
    return result
  else:
    return result, proc.returncode


class EMRLib:

  def __init__(self, ec2_key_file, ec2_key_name, region='us-east-1'):
    self.ec2_key_file = ec2_key_file
    self.ec2_key_name = ec2_key_name
    self.emr_client = boto3.client('emr', region_name=region)
    self.rds_client = boto3.client('rds', region_name=region)
    self.s3_client = boto3.client('s3', region_name=region)
    _, self.ssh_tmp_hosts_file = tempfile.mkstemp()

  def wait_for_services(self, services):
    '''Waits and pools until all the items in 'services' have is_ready() == True'''
    if len(services) == 0:
      return
    print('Waiting for {n!s} services to start...'.format(n=len(services)))
    while True:
      i = 0
      while i < len(services):
        if services[i].is_ready():
          print('{name!s} is ready, waiting for {n!s} more services to start...'.format(
              name=services[i],
              n=(len(services) - 1)
          ))
          services = services[:i] + services[i + 1:]  # remove ready service
        else:
          i += 1
        if len(services) == 0:
          return
      time.sleep(15)

  def create_or_connect_to_emr_cluster(
          self, name, log_uri, owner, expiry,
          instance_count=2,
          hdfs_replication='2'):
    list = self.emr_client.list_clusters(
        ClusterStates=['RUNNING', 'WAITING'])
    for cluster in list['Clusters']:
      if cluster['Name'] == name:
        cluster_id = cluster['Id']
        print('Reusing existing cluster: ' + cluster_id)
        return EMRCluster(cluster_id, self)
    print('Creating new cluster.')
    # We're passing these options to the namenode and to the hdfs datanodes so
    # that they will make their monitoring data accessible via the jmx interface.
    jmx_options = '"-Dcom.sun.management.jmxremote ' \
        '-Dcom.sun.management.jmxremote.authenticate=false ' \
        '-Dcom.sun.management.jmxremote.ssl=false ' \
        '-Dcom.sun.management.jmxremote.port={port} ${{{name}}}"'
    res = self.emr_client.run_job_flow(
        Name=name,
        LogUri=log_uri,
        ReleaseLabel='emr-5.2.0',
        Instances={
            'MasterInstanceType': 'm3.2xlarge',
            'SlaveInstanceType': 'm3.2xlarge',
            'InstanceCount': instance_count,
            'Ec2KeyName': self.ec2_key_name,
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': True
        },
        Configurations=[
            {
                'Classification': 'mapred-site',
                'Properties': {
                    'mapred.output.committer.class': 'org.apache.hadoop.mapred.FileOutputCommitter'
                }
            },
            {
                'Classification': 'yarn-site',
                'Properties': {
                    'yarn.nodemanager.container-monitor.procfs-tree.smaps-based-rss.enabled': 'true'
                }
            },
            {
                'Classification': 'hadoop-env',
                'Properties': {},
                'Configurations': [
                    {
                        'Classification': 'export',
                        'Properties': {
                            'HADOOP_NAMENODE_OPTS': jmx_options.format(port=8004, name='HADOOP_NAMENODE_OPTS'),
                            'HADOOP_DATANODE_OPTS': jmx_options.format(port=8005, name='HADOOP_DATANODE_OPTS')
                        }
                    }
                ]
            },
            {
                'Classification': 'hdfs-site',
                'Properties': {
                    'dfs.replication': hdfs_replication
                }
            }
        ],
        JobFlowRole="EMR_EC2_DefaultRole",
        VisibleToAllUsers=True,
        ServiceRole="EMR_DefaultRole",
        Tags=[{
              'Key': 'owner',
              'Value': owner
              },
              {
              'Key': 'expiry',
              'Value': expiry
              }])
    return EMRCluster(res['JobFlowId'], self)

  def create_or_connect_to_rds_instance(self, name):
    if RDSInstance.get_description(self.rds_client, name) is None:
      print('Creating new DB instance.')
      self.rds_client.create_db_instance(
          Engine='mysql',
          DBInstanceIdentifier=name,
          BackupRetentionPeriod=0,
          DBName='db',
          MasterUsername='root',
          MasterUserPassword='rootroot',
          DBInstanceClass='db.m3.2xlarge',
          AllocatedStorage=50)
    else:
      print('Reusing existing DB instance')
    return RDSInstance(name, self)


class RDSInstance:
  '''Represents a connection to an RDS instance.'''

  def __init__(self, name, lib):
    self.name = name
    self.client = lib.rds_client

  def __str__(self):
    return 'RDS({name!s})'.format(name=self.name)

  @staticmethod
  def get_description(client, name):
    try:
      response = client.describe_db_instances(
          DBInstanceIdentifier=name)
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] != 'DBInstanceNotFound':
        raise e
      return None

    for instance in response['DBInstances']:
      if instance['DBInstanceStatus'] == 'available':
        return instance
    return None

  def get_address(self):
    return self.get_description(self.client, self.name)['Endpoint']['Address']

  def is_ready(self):
    desc = self.get_description(self.client, self.name)
    return desc is not None

  def terminate(self):
    self.client.delete_db_instance(
        DBInstanceIdentifier=self.name,
        SkipFinalSnapshot=True)


class EMRCluster:
  '''Represents a connection to an EMR cluster'''

  def __init__(self, id, lib):
    self.id = id
    self.emr_client = lib.emr_client
    self.ssh_cmd = [
        'ssh',
        '-T',
        '-i', lib.ec2_key_file,
        '-o', 'UserKnownHostsFile=' + lib.ssh_tmp_hosts_file,
        '-o', 'CheckHostIP=no',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'ServerAliveInterval=30',
    ]
    self._master = None

  def __str__(self):
    return 'EMR(' + self.id + ')'

  def desc(self):
    '''Raw description of the cluster.'''
    return self.emr_client.describe_cluster(ClusterId=self.id)

  def master(self):
    '''DNS name of the master host.'''
    if self._master is None:
      desc = self.desc()
      if self.is_ready(desc=desc):
        self._master = desc['Cluster']['MasterPublicDnsName']
    return self._master

  def is_ready(self, desc=None):
    '''Is the cluster started up and ready?'''
    if desc is None:
      desc = self.desc()
    state = desc['Cluster']['Status']['State']
    return state == 'RUNNING' or state == 'WAITING'

  def ssh(self, cmds, print_output=True, verbose=True, assert_successful=True):
    '''Send shell commands to the cluster via invoking ssh.'''
    if verbose:
      def trunc(s):
        s = s.replace('\n', ' ')
        if len(s) >= 97:
          return s[:97] + '...'
        else:
          return s
      print('[EMR EXECUTE] {cmd!s}'.format(cmd=trunc(cmds)))
    # Stop on errors by default.
    cmds = 'set -e\n' + cmds
    return call_cmd(
        self.ssh_cmd + ['hadoop@' + self.master()],
        input=cmds,
        print_output=print_output,
        assert_successful=assert_successful)

  def ssh_nohup(
          self,
          cmds,
          script_file='/home/hadoop/run_cmd.sh',
          output_file='/home/hadoop/cmd_output.txt',
          status_file='/home/hadoop/cmd_status.txt',
          print_output=True,
          verbose=True):
    '''Send shell commands to the cluster via ssh, and run them with nohup in
    the background.'''
    self.ssh('''
      cat >{script_file!s} <<'EOF'
        {cmds!s}
        echo "done" >{status_file!s}
EOF
      rm -f {status_file!s}
      nohup bash {script_file!s} >{output_file!s} 2>&1 &
    '''.format(
        cmds=cmds,
        script_file=script_file,
        output_file=output_file,
        status_file=status_file))

  def fetch_output(
          self,
          output_file='/home/hadoop/cmd_output.txt',
          status_file='/home/hadoop/cmd_status.txt'):
    '''Periodically connects to the master and downloads and prints
    the output log of the running script. Also monitors a status
    file at the master, and quits the loop in case of done status.
    It would be simpler to have a continuous ssh connection to the
    master, but that breaks if the Internet connection flakes.'''
    all_output = ''
    output_lines_seen = 0
    status_is_done = False
    while not status_is_done:
      # Check status.
      status, ssh_retcode = self.ssh(
          'cat ' + status_file + ' 2>/dev/null',
          assert_successful=False,
          print_output=False,
          verbose=False)
      status_is_done = ssh_retcode == 0 and 'done' == status.strip()
      # Print unseen log lines.
      output_results, return_code = self.ssh(
          'tail -n +{offset!s} {output_file!s}'.format(
              output_file=output_file,
              offset=output_lines_seen + 1),
          assert_successful=False,
          verbose=False,
          print_output=False)
      if return_code == 0:
        # We only use the output of ssh if it was successful. Otherwise we'll
        # try again with the same offset in the next round.
        print(output_results, end='')
        all_output += output_results
        output_lines_seen += output_results.count('\n')
      time.sleep(5)
    return all_output

  def rsync_up(self, src, dst):
    '''Copy files to the cluster via invoking rsync.'''
    print('[EMR UPLOAD] {src!s} TO {dst!s}'.format(src=src, dst=dst))
    call_cmd(
        [
            'rsync',
            '-ave',
            ' '.join(self.ssh_cmd),
            '-r',
            '--copy-dirlinks',
            src,
            'hadoop@' + self.master() + ':' + dst
        ])

  def rsync_down(self, src, dst):
    '''Copy files from the cluster via invoking rsync.'''
    print('[EMR DOWNLOAD] {src!s} TO {dst!s}'.format(src=src, dst=dst))
    call_cmd(
        [
            'rsync',
            '-ave',
            ' '.join(self.ssh_cmd),
            '-r',
            '--copy-dirlinks',
            'hadoop@' + self.master() + ':' + src,
            dst
        ])

  def turn_termination_protection_off(self):
    self.emr_client.set_termination_protection(
        JobFlowIds=[self.id],
        TerminationProtected=False)

  def terminate(self):
    self.emr_client.terminate_job_flows(
        JobFlowIds=[self.id])
