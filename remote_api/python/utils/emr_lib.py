# Utility API for starting EMR and RDS instances, and manipulating them with
# ssh and rsync.

import boto3
import botocore
import subprocess
import sys
import tempfile
import time


def call_cmd(cmd_list, input=None, print_output=True):
  '''
  Invoked an OS command with arguments.
  cmd_list: the command and its arguments in a list
  input: A string that is passed to the stding of the invoked
    process.
  print_ouput: Whether to print the output (stdout+stderr) of the
    process to stdout of Python.
  returns: A tuple of the combined stdout+stderr and the return code
    of the process.
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
  while True:
    line = proc.stdout.readline()
    result += line
    if print_output:
      print(line, end='')
    if not line:
      break
  while proc.poll() is None:
    time.sleep(0.1)
  return result, proc.returncode


class EMRLib:

  def __init__(self, ec2_key_file, ec2_key_name):
    self.ec2_key_file = ec2_key_file
    self.ec2_key_name = ec2_key_name
    self.emr_client = boto3.client('emr')
    self.rds_client = boto3.client('rds')
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

  def create_or_connect_to_emr_cluster(self, name):
    list = self.emr_client.list_clusters(
        ClusterStates=['RUNNING', 'WAITING'])
    for cluster in list['Clusters']:
      if cluster['Name'] == name:
        cluster_id = cluster['Id']
        print('Reusing existing cluster: ' + cluster_id)
        return EMRCluster(cluster_id, self)
    print('Creating new cluster.')
    res = self.emr_client.run_job_flow(
        Name=name,
        ReleaseLabel="emr-4.7.2",
        Instances={
            'MasterInstanceType': 'm3.xlarge',
            'SlaveInstanceType': 'm3.xlarge',
            'InstanceCount': 2,
            'Ec2KeyName': self.ec2_key_name,
            'KeepJobFlowAliveWhenNoSteps': True
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        VisibleToAllUsers=True,
        ServiceRole="EMR_DefaultRole")
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
          AllocatedStorage=20)
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

  def ssh(self, cmds, print_output=True, verbose=True):
    '''Send shell commands to the cluster via invoking ssh.'''
    if verbose:
      def trunc(s):
        s = s.replace('\n', ' ')
        if len(s) >= 97:
          return s[:97] + '...'
        else:
          return s
      print('[EMR EXECUTE] {cmd!s}'.format(cmd=trunc(cmds)))
    return call_cmd(
        self.ssh_cmd + ['hadoop@' + self.master()],
        input=cmds,
        print_output=print_output)

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

  def terminate(self):
    self.emr_client.terminate_job_flows(
        JobFlowIds=[self.id])
