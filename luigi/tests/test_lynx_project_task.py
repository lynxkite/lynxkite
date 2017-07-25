import unittest
import lynx.luigi
from unittest import mock


lk = lynx.kite.LynxKite()


class TestTask(lynx.luigi.ProjectTask):

  def compute_project(self):
    return self.lk.new_project().examplegraph()

  def output_name(self):
    return 'ILoveProjectTestingSoMuch'


class TestProject(unittest.TestCase):

  def test_run(self):
    lk._request('/ajax/discardAllReallyIMeanIt')
    tt = TestTask()
    tt.run()
    self.assertTrue(tt.output().exists())


if __name__ == '__main__':
  unittest.main()
