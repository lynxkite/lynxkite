import lynx.luigi
import os
import shutil
import unittest


class DefaultPartitionsTask(lynx.luigi.LynxTableFileTask):

  def compute_view(self):
    p = self.lk.new_project()
    p.newVertexSet(size=1000)
    sql = self.lk.sql("select * from `p`", p=p)
    return sql


class MorePartitionsTask(DefaultPartitionsTask):
  shuffle_partitions = 500


class TestLynxTableFileTask(unittest.TestCase):

  def do_test_partitioning(self, task_class, num_partitions):
    task = task_class()
    task.run()
    resolved_path = task.output().resolved_path()
    raw_path = resolved_path[len('file:'):]
    files = os.listdir(raw_path)
    num_files = len(list(filter(lambda file: file.endswith('.snappy.parquet'), files)))
    assert(num_files == num_partitions)
    # Clean up, if everything was okay
    shutil.rmtree(raw_path)

  def test_default_partition(self):
    self.do_test_partitioning(DefaultPartitionsTask, 1)

  def test_more_partitions(self):
    self.do_test_partitioning(MorePartitionsTask, MorePartitionsTask.shuffle_partitions)

if __name__ == '__main__':
  unittest.main()
