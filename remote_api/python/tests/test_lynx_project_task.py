import unittest
import lynx.luigi
from unittest import mock
import types


class TestTask(lynx.luigi.ProjectTask):

  def compute_project(self):
    return self.lk.new_project().examplegraph()

  def output_name(self):
    return 'ILoveTestingSoMuch'


class TestProject(unittest.TestCase):

  def test_run(self):
    tt = TestTask()
    try:
      tt.run()
    except lynx.LynxException:
      pass
    self.assertTrue(tt.output().exists())


if __name__ == '__main__':
  unittest.main()
