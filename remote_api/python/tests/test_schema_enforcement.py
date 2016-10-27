import unittest
import lynx.luigi
from unittest import mock
import types
import shutil


lk = lynx.LynxKite()

query = '''SELECT false AS zero, 1 AS one, 2.0 AS two, 'three' AS three'''
directory = r'tests/ThisDirectoryDoesNotExistYet'


class TestTask:

  def inputview(self):
    return self.lk.sql(query)

  def compute_view(self):
    return self.inputview()


class TestTaskNoCheck(TestTask, lynx.luigi.SchemaEnforcedTask):
  schema_directory = None


class TestTaskDoCheck(TestTask, lynx.luigi.SchemaEnforcedTask):
  schema_directory = directory


class TestTable(unittest.TestCase):

  def test_run(self):
    lk._request('/ajax/discardAllReallyIMeanIt')

    tt = TestTaskNoCheck()
    tt.run()

    tt = TestTaskDoCheck()
    # directory does not exist and should be created
    tt.run()
    # directory exists
    tt.run()
    # directory exists, but contains a different schema
    shutil.copy(r"tests/TestTaskDoCheck.schema", directory + "/TestTaskDoCheck.schema")
    self.assertRaises(AssertionError, tt.run)

  def tearDown(self):
    shutil.rmtree(directory, ignore_errors=True)

if __name__ == '__main__':
  unittest.main()
