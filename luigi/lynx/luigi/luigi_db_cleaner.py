'''Periodic cleanup task.'''
import datetime
import luigi
import lynx.luigi.luigi_db
import os


class LuigiDbCleaner(luigi.Task):
  '''Cleans up old entries in the Luigi database.'''
  date = luigi.DateMinuteParameter()

  def output(self):
    '''Marker file for the day's run. This is on local disk because losing it is not a problem.'''
    target_file = 'tmp/LuigiDbCleaner-run-at-{:%Y%m%d-%H%M}'.format(self.date)
    return luigi.LocalTarget(target_file)

  def run(self):
    config_file = os.path.join(
        os.environ['LYNX'],
        'config/luigi.cfg')
    with lynx.luigi.luigi_db.create_mysql_connection(config_file) as conn:
      # Delete old events.
      lynx.luigi.luigi_db.commit_sql(
          conn,
          'DELETE FROM task_events WHERE ts < NOW() - INTERVAL 4 MONTH')
      # Delete tasks without events.
      lynx.luigi.luigi_db.commit_sql(
          conn,
          'DELETE tasks.* FROM tasks LEFT JOIN task_events  ON tasks.id = task_events.task_id WHERE task_events.task_id IS NULL')
      # Delete parameters without tasks.
      lynx.luigi.luigi_db.commit_sql(
          conn,
          'DELETE task_parameters.* FROM task_parameters LEFT JOIN tasks ON task_parameters.task_id = tasks.id WHERE tasks.id IS NULL')
    with self.output().open('w') as f:
      pass  # Just create marker file.
