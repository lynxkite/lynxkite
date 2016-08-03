#!/usr/bin/python3

import boto3
import subprocess
import tempfile

print('hello, world')

emr_client = boto3.client('emr')

print(emr_client.waiter_names)

_, ssh_tmp_hosts_file = tempfile.mkstemp() 

def call_cmd(cmd_list, input=None):
  print('CALL ', cmd_list)
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
  #print(stdout)
  while True:
    line = proc.stdout.readline()
    print(line, end='')
    if not line: break

class EMRCluster:
  def __init__(self, id):
    self.id = id
    desc = emr_client.describe_cluster(ClusterId=id)
    print(desc)
    self.master = desc['Cluster']['MasterPublicDnsName']
    print(self.master)
    self.ssh_cmd = [
          'ssh',
          '-T',
          '-i', '/home/gfeher/.ssh/lynx-cli.pem',
          '-o', 'UserKnownHostsFile=' + ssh_tmp_hosts_file,
          '-o', 'CheckHostIP=no',
          '-o', 'StrictHostKeyChecking=no',
          '-o', 'ServerAliveInterval=30',
        ]

  def ssh(self, cmds):
    call_cmd(self.ssh_cmd + ['hadoop@' + self.master], input=cmds)

  def rsync_up(self, src, dst):
    call_cmd(
        [
          'rsync',
          '-ave',
          ' '.join(self.ssh_cmd),
          '-r',
          '--copy-dirlinks',
          src,
          'hadoop@' + self.master + ':' + dst
        ])
    
    

  @staticmethod
  def connect(name='gfeher-boto-cluster'):
    l = emr_client.list_clusters(ClusterStates=['RUNNING','WAITING'])
    for cluster in l['Clusters']:
      print(cluster)
      if cluster['Name'] == name:
        print('reusing')
        return EMRCluster(cluster['Id'])
    print('creating')
    res = emr_client.run_job_flow(
      Name="gfeher-boto-cluster",
      ReleaseLabel="emr-4.7.2",
      Instances={
          'MasterInstanceType': 'm3.xlarge',
          'SlaveInstanceType': 'm3.xlarge',
          'InstanceCount': 2,
          'Ec2KeyName':'lynx-cli',
          'KeepJobFlowAliveWhenNoSteps':True
      },
      JobFlowRole="EMR_EC2_DefaultRole",
      VisibleToAllUsers=True,
      ServiceRole="EMR_DefaultRole")

    print(res)

    waiter = emr_client.get_waiter('cluster_running')
    waiter.wait(ClusterId=res['JobFlowId'])
    return EMRCluster(res['JobFlowId'])

cc = EMRCluster.connect()
print(cc)
cc.ssh('''
set -x
cd /mnt

export SIGNED='https://d955izgyg907d.cloudfront.net/lynx-1.9.0.tgz?Expires=1501238130&Signature=CbukS7jQ8TjSjZZIACl0AXPpag61iDA1tpnvkjcd~eJq0ZTCOBKNxyfLpcTLTrHdTh-VToI9YXfcr2GvCKZ-z8iXhidXwmJNyywhjelTpEyIybBpOTPbmd1oIyL1zz84sR1-YxQ~xm78MAyJV8n5NTq7a7qL5qBkbuVkec1kc3mw-sSl-ncoARV6m3woiIooHYuBaO9sNd25z5lfCizdhed4g44RDZ0GC1GCu7zeuCxZTHKdl2O6Z-~MCGb0GDeBBNpoHMHVd5VaSs8IweqaXmhueYMOdDHMD3Bduq5gxZww7Ei3uQ-bjUV~plsb0O2uA3lUEt8YisSB7Ze-HhH2ew__&Key-Pair-Id=APKAJBDZZHY2ZM7ELY2A'

if [ ! -d "./lynx-1.9.0" ]; then
  wget -q --show-progress "$SIGNED" -O 'lynx-1.9.0.tgz'
  tar xfz lynx-1.9.0.tgz
fi

cd lynx-1.9.0

if [ hash docker 2>/dev/null ]; then
  echo "docker was found!"
else
  sudo LYNXKITE_USER=hadoop ./install-docker.sh
fi

cd lynx/luigi_tasks
# no need for scheduled tasks today. just have sthg dummy
echo "scheduling: []" >chronomaster.yml

mkdir test_tasks || true
''')

cc.rsync_up(
  'ecosystem/tests/',
  '/mnt/lynx-1.9.0/lynx/luigi_tasks/test_tasks')

cc.ssh('''
  cd /mnt/lynx-1.9.0/lynx

  ./start.sh

  echo "start OK"

  docker exec -i lynx_luigi_worker_1 luigi \
    --module test_tasks.smoke_test \
    LynxKiteBackup \
    --date '2016-01-01T1010'

  echo "docker OK"

''')

