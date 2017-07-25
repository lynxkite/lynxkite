import unittest
import lynx.luigi
from unittest import mock
import types
import os


lk = lynx.kite.LynxKite()


class TestTask(lynx.luigi.TableTask):

  def setup(self, path):
    import sqlite3
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.executescript("""
    DROP TABLE IF EXISTS testtable;
    CREATE TABLE testtable
    (id INTEGER, name TEXT);
    INSERT INTO testtable VALUES
    (1, 'Test1'),
    (2, 'Test2'),
    (3, 'Test3'),
    (4, 'Test4');
    """)
    conn.commit()
    conn.close()

  def compute_view(self):
    path = os.path.abspath('tests/test.db')
    try:
      os.remove(path)
    except OSError:
      pass
    self.setup(path)
    url = 'jdbc:sqlite:{}'.format(path)
    view = lk.import_jdbc(
        jdbcUrl=url,
        jdbcTable='testtable',
        keyColumn='id')
    return view

  def output_name(self):
    return 'ILoveTableTestingSoMuch'


class TestTable(unittest.TestCase):

  def test_run(self):
    lk._request('/ajax/discardAllReallyIMeanIt')
    tt = TestTask()
    tt.run()
    self.assertTrue(tt.output().exists())


if __name__ == '__main__':
  unittest.main()
