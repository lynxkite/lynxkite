import unittest
import luigi
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


class TestTarget(luigi.Target):
  _lk_for_exists = lk

  def exists(self):
    return True

  def view(self, lk):
    return lk.sql(query)

  def list_files(self):
    return []

  def read_file(self, filename):
    pass

  def create_directory(self):
    pass

  def write_file(self, filename, stream):
    pass


class TestTask(luigi.Task):

  def output(self):
    return TestTarget()


class TestTaskNoCheck(lynx.luigi.SchemaEnforcedTransferTask):
  schema_directory = None

  def requires(self):
    return TestTask()

  def output(self):
    return TestTarget()


class TestTaskDoCheck(lynx.luigi.SchemaEnforcedTransferTask):
  schema_directory = directory

  def requires(self):
    return TestTask()

  def output(self):
    return TestTarget()


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
