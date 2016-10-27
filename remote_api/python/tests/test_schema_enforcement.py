import unittest
import lynx.luigi
from unittest import mock
import types
import os


lk = lynx.LynxKite()

query = '''SELECT false AS zero, 1 AS one, 2.0 AS two, 'three' AS three'''


class TestTask:

  def inputview(self):
    return self.lk.sql(query)

  def compute_view(self):
    return self.inputview()


class TestTaskNoCheck(TestTask, lynx.luigi.SchemaEnforcedTask):
  schema_directory = None


class TestTaskDoCheckNoDir(TestTask, lynx.luigi.SchemaEnforcedTask):
  schema_directory = r'ThisDirectoryDoesNotExistYet'


class TestTable(unittest.TestCase):

  def test_run(self):
    lk._request('/ajax/discardAllReallyIMeanIt')

    tt = TestTaskNoCheck()
    tt.run()

    tt = TestTaskDoCheckNoDir()
    tt.run()
    tt.run()


if __name__ == '__main__':
  unittest.main()
