'''Periodic cleanup task.'''
import datetime
import luigi
import lynx.luigi
import lynx.util
import subprocess
import types


class ValkyrieCleanup(lynx.luigi.LynxTask):
  '''Cleans up old ``LynxTableFileTarget``s.'''
  date = luigi.DateMinuteParameter()
  # The (prefixed) directories to keep clean.
  DIRECTORIES = ['DATA$/table_files', 'DATA$/uploads']

  def output(self):
    '''Marker file for the day's run. This is on local disk because losing it is not a problem.'''
    return luigi.LocalTarget('tmp/valkyrie-run-at-{:%Y%m%d-%H%M}'.format(self.date))

  def run(self):
    '''Finds and deletes old paths within ``DIRECTORIES``.'''
    for dir in self.DIRECTORIES:
      path = self.lk.get_prefixed_path(dir).resolved
      for e in lynx.util.HDFS.list(path, options='-R'):
        # Check if it's a directory (this doesn't seem to be possible with hadoop fs -ls)
        if not e.permission.startswith('d'):
          continue
        ttl = lynx.util.get_ttl_from_path(e.path)
        if ttl and e.datetime + ttl < self.date:
          lynx.util.HDFS.rm(e.path)
    with self.output().open('w') as f:
      pass  # Just create marker file.
