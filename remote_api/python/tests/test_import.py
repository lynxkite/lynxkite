import unittest
import lynx
import os


class TestImport(unittest.TestCase):

  """
  All import calls use the same backend function,
  no need to test them separately.
  """

  def setup(self, path):
    import sqlite3
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.executescript("""
    DROP TABLE IF EXISTS subscribers;
    CREATE TABLE subscribers
    (n TEXT, id INTEGER, name TEXT, gender TEXT, 'race condition' TEXT, level DOUBLE PRECISION);
    INSERT INTO subscribers VALUES ('A', 1, 'Daniel', 'Male', 'Halfling', 10.0);
    INSERT INTO subscribers VALUES ('B', 2, 'Beata', 'Female', 'Dwarf', 20.0);
    INSERT INTO subscribers VALUES ('C', 3, 'Felix', 'Male', 'Gnome', NULL);
    INSERT INTO subscribers VALUES ('D', 4, 'Oliver', 'Male', 'Troll', NULL);
    INSERT INTO subscribers VALUES (NULL, 5, NULL, NULL, NULL, NULL);
    INSERT INTO subscribers VALUES (NULL, 6, '☃', 'Unicode', 'Snowman', 999.999);
    """)
    conn.commit()
    conn.close()

  def stub_test_jdbc(self):
    path = os.path.abspath('tests/test.db')
    self.setup(path)
    url = 'jdbc:sqlite:{}'.format(path)
    lk = lynx.LynxKite()
    lk._request('/ajax/discardAllReallyIMeanIt')
    view = lk.import_jdbc(
        jdbcUrl=url,
        jdbcTable='subscribers',
        keyColumn='id')
    res = lk.sql('select * from `cp` order by id', cp=view)
    self.check_result(res)

  def stub_test_jdbc_predicates(self):
    path = os.path.abspath('tests/test.db')
    self.setup(path)
    url = 'jdbc:sqlite:{}'.format(path)
    lk = lynx.LynxKite()
    lk._request('/ajax/discardAllReallyIMeanIt')
    view = lk.import_jdbc(
        jdbcUrl=url,
        jdbcTable='subscribers',
        predicates=['id<=2', 'id>=3'])
    res = lk.sql('select * from `cp` order by id', cp=view)
    self.check_result(res)

  def stub_test_jdbc_partitions(self):
    path = os.path.abspath('tests/test.db')
    self.setup(path)
    url = 'jdbc:sqlite:{}'.format(path)
    lk = lynx.LynxKite()
    lk._request('/ajax/discardAllReallyIMeanIt')
    view = lk.import_jdbc(
        jdbcUrl=url,
        jdbcTable='subscribers',
        keyColumn='id',
        numPartitions=2)
    res = lk.sql('select * from `cp` order by id', cp=view)
    self.check_result(res)

  def check_result(self, res):
    self.assertEqual(
        res.take(100),
        [{'gender': 'Male',
          'level': 10.0,
          'name': 'Daniel',
          'id': 1,
          'race condition': 'Halfling', 'n': 'A'},
         {'gender': 'Female',
          'level': 20.0,
          'name': 'Beata',
          'id': 2,
          'race condition': 'Dwarf', 'n': 'B'},
         {'gender': 'Male',
          'id': 3,
          'n': 'C',
          'name': 'Felix',
          'race condition': 'Gnome'},
         {'gender': 'Male',
          'id': 4,
          'n': 'D',
          'name': 'Oliver',
          'race condition': 'Troll'},
         {'id': 5},
         {'gender': 'Unicode',
          'id': 6,
          'name': '☃',
          'race condition': 'Snowman',
          'level': 999.999},
         ])

  def test_jdbc_view(self):
    self.stub_test_jdbc()
    self.stub_test_jdbc_predicates()
    self.stub_test_jdbc_partitions()


if __name__ == '__main__':
  unittest.main()
