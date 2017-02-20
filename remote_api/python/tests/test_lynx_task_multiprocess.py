import unittest
import luigi
import lynx.luigi
from unittest import mock


lk = lynx.LynxKite()


class TestTask(lynx.luigi.ProjectTask):
  name = luigi.Parameter()

  def compute_project(self):
    return self.lk.new_project().examplegraph()

  def output_name(self):
    return self.name


class TestLynxTasksWithMultiprocessing(unittest.TestCase):

  def test_run(self):
    lk._request('/ajax/discardAllReallyIMeanIt')
    tasks = [TestTask(name='TestProject' + str(i)) for i in range(3)]
    # Run the above tasks with two workers.
    luigi.build(tasks, workers=2, local_scheduler=True)
    # If LynxKite objects don't handle multiprocessing correctly,
    # then the above call can enter an endless loop or fail with
    # an exception.
    for task in tasks:
      assert(task.output().exists())


if __name__ == '__main__':
  unittest.main()
