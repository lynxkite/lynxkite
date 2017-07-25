import luigi
from luigi.contrib.hdfs.target import HdfsTarget
import os


class LynxKiteBackup(luigi.Task):
  '''Creates hourly backups of the LynxKite metadata directory to HDFS.'''
  date = luigi.DateMinuteParameter()
  metadata = luigi.Parameter()

  def filename(self):
    # The date truncated to the hour.
    d = self.date.strftime('%Y%m%d%H%M')
    return 'lynxkite-backup-{}.tgz'.format(d)

  def directory(self):
    return os.environ['HDFS_ROOT'] + '/backups/'

  def output(self):
    return HdfsTarget(self.directory() + self.filename())

  def run(self):
    import subprocess
    retcode = subprocess.call(
        ['tar', '-cz', '--warning=no-file-changed', '-f', self.filename(), self.metadata])
    print('tar return code:', retcode)
    if retcode not in [0, 1]:
      raise Exception('Fatal error while invoking tar, return code is', retcode)
    # Ignore error code for directory creation. It may already exist.
    subprocess.call(['hadoop', 'fs', '-mkdir', self.directory()])
    subprocess.check_call(
        ['hadoop', 'fs', '-put', self.filename(), self.output().path])
    subprocess.check_call(['rm', self.filename()])  # clean up temporary file
