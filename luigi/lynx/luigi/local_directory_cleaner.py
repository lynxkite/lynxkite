'''Periodic cleanup task.'''
import datetime
import luigi
import lynx.util
import os
import shutil


class LocalDirectoryCleaner(luigi.Task):
  '''Cleans up old entities in the specified directory.'''
  date = luigi.DateMinuteParameter()
  directory = luigi.Parameter()

  def output(self):
    '''Marker file for the day's run. This is on local disk because losing it is not a problem.'''
    prefix = self.directory.replace('/', '_')
    target_file = 'tmp/LocalDdirectoryCleaner-{}-run-at-{:%Y%m%d-%H%M}'.format(prefix, self.date)
    return luigi.LocalTarget(target_file)

  def run(self):
    '''Finds and deletes old entities in ``self.directory``.'''
    for dirpath, dirnames, filenames in os.walk(self.directory, topdown=True):
      for dirname in dirnames:
        self.remove_if_too_old(dirpath, dirname, shutil.rmtree)
      for filename in filenames:
        self.remove_if_too_old(dirpath, filename, os.remove)
    with self.output().open('w') as f:
      pass  # Just create marker file.

  def remove_if_too_old(self, dirpath, entity, rm_fn):
    path = os.path.join(dirpath, entity)
    if self.is_too_old(path):
      rm_fn(path)

  def is_too_old(self, path):
    entity = os.path.basename(path)
    ttl = lynx.util.get_ttl_from_path(entity)
    if ttl:
      ctime = os.path.getctime(path)
      creation_date = datetime.datetime.fromtimestamp(ctime)
      return creation_date + ttl < self.date
    else:
      return False
