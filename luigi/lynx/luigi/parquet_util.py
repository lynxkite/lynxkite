import chronomaster_lib
import yaml
import argparse
import contextlib
import sqlite3
import lynx.kite
import lynx.luigi


def collect_task_counts(task, count_list, lk):
  if isinstance(task.output(), lynx.luigi.PrefixedTarget) and \
          task.output().exists():
    path = task.output().path
    try:
      yield (task.task_id, lk.get_parquet_metadata(path).rowCount)
    except:
      # We don't want to stop just because one specific file failed.
      pass
  for t in task._requires():
    yield from collect_task_counts(t, count_list, lk)


def get_row_counts(config_file, date):
  """Reads the row counts from the parquet metadata of the tasks that were run
  on the specified day according to the specified configuration file."""
  with open(config_file) as config_yaml:
    config = yaml.load(config_yaml)
  task_instances = chronomaster_lib.get_all_task_instances_for_given_day(config, date)
  lk = lynx.kite.LynxKite()
  count_list = []
  for t in task_instances:
    yield from collect_task_counts(t.to_luigi_task(), count_list, lk)


def save_row_counts_to_db(records):
  """Saves the parquet file paths and the respective row counts to the database."""
  task_db = sqlite3.connect(chronomaster_lib.task_db_path)
  with contextlib.closing(task_db.cursor()) as c:
    for task_id, row_count in records:
      c.execute('INSERT OR REPLACE INTO parquet_output(task_id, row_count) VALUES(?, ?)',
                (task_id, row_count))
    task_db.commit()
