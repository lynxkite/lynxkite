import boto3
import botocore
import subprocess
import sys
import tempfile
import time


def call_cmd(cmd_list, input=None):
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
  while True:
    line = proc.stdout.readline()
    print(line, end='')
    if not line:
      break


def trunc(s):
  s = s.replace('\n', ' ')
  if len(s) >= 97:
    return s[:97] + '...'
  else:
    return s


class EMRLib:

  def __init__(self, ec2_key_file, ec2_key_name):
    self.ec2_key_file = ec2_key_file
    self.ec2_key_name = ec2_key_name
    self.emr_client = boto3.client('emr')
    self.rds_client = boto3.client('rds')
    _, self.ssh_tmp_hosts_file = tempfile.mkstemp()

  def wait_for_services(self, services):
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
      time.sleep(30)

  def create_or_connect(self, name):
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

  def create_or_connect_db(self, name):
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


class EMRCluster:

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
    self._desc = None

  def __str__(self):
    return 'EMR(' + self.id + ')'

  def desc(self):
    if not self._desc:
      self._desc = self.emr_client.describe_cluster(ClusterId=self.id)
    return self._desc

  def master(self):
    return self.desc()['Cluster']['MasterPublicDnsName']

  def is_ready(self):
    res = self.desc()
    state = res['Cluster']['Status']['State']
    return state == 'RUNNING' or state == 'WAITING'

  def ssh(self, cmds):
    print('[EMR EXECUTE] {cmd!s}'.format(cmd=trunc(cmds)))
    call_cmd(self.ssh_cmd + ['hadoop@' + self.master()], input=cmds)

  def rsync_up(self, src, dst):
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
