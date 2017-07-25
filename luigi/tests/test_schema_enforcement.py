import unittest
import lynx.luigi
from unittest import mock
import types
import shutil
import yaml


lk = lynx.kite.LynxKite()

query = '''SELECT false AS zero, 1 AS one, 2.0 AS two, 'three' AS three'''
directory = r'tests/ThisDirectoryDoesNotExistYet'
with open(r"tests/TestTaskDoCheck.schema.correct", 'r') as f:
  correct_schema = yaml.load(f)


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
    with open(directory + "/TestTaskDoCheck.schema", 'r') as f:
      self.assertEqual(correct_schema, yaml.load(f))
    # directory exists
    tt.run()
    with open(directory + "/TestTaskDoCheck.schema", 'r') as f:
      self.assertEqual(correct_schema, yaml.load(f))
    # directory exists, but contains a different schema
    shutil.copy(r"tests/TestTaskDoCheck.schema.incorrect", directory + "/TestTaskDoCheck.schema")
    self.assertRaises(AssertionError, tt.run)

  def tearDown(self):
    shutil.rmtree(directory, ignore_errors=True)

if __name__ == '__main__':
  unittest.main()
