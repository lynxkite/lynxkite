"""
This module defines a luigi task for reading parquet row counts
and saving them into a database.
"""

import datetime
import luigi
import lynx.luigi.parquet_util


class StoreRowCounts(luigi.task.Task):
  """
  A task for getting row counts from parquet files. First, it reads the list of
  tasks from the chronomaster configuration file specified by ``config_file`` and instantiates
  them with the previous day as ``date``, reads the row_counts from the corresponding files,
  then it stores them in the database specified by ``task_db_path`` in chronomaster_lib.py.
  """
  date = luigi.DateMinuteParameter()
  config_file = luigi.Parameter()

  def run(self):
    row_counts = lynx.luigi.parquet_util.get_row_counts(
        self.config_file, self.date.date() - datetime.timedelta(days=1)
    ) or []
    lynx.luigi.parquet_util.save_row_counts_to_db(row_counts)
    with self.output().open('w') as f:
      pass  # Just create marker file.

  def output(self):
    '''Marker file for the day's run. This is on local disk because losing it is not a problem.'''
    return luigi.LocalTarget('tmp/store-row-count-run-at-{:%Y%m%d-%H%M}'.format(self.date))
