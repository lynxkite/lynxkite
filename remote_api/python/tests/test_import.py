import unittest
import lynx
import os


class TestImport(unittest.TestCase):

  """
  All import calls use the same backend function,
  no need to test them separately.
  """

  def stub_test_jdbc(self, view):
    import sqlite3
    path = os.path.abspath("tests/test.db")
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.executescript("""
    DROP TABLE IF EXISTS subscribers;
    CREATE TABLE subscribers
    (n TEXT, id INTEGER, name TEXT, gender TEXT, "race condition" TEXT, level DOUBLE PRECISION);
    INSERT INTO subscribers VALUES
    ('A', 1, 'Daniel', 'Male', 'Halfling', 10.0),
    ('B', 2, 'Beata', 'Female', 'Dwarf', 20.0),
    ('C', 3, 'Felix', 'Male', 'Gnome', NULL),
    ('D', 4, 'Oliver', 'Male', 'Troll', NULL),
    (NULL, 5, NULL, NULL, NULL, NULL);
    """)
    conn.commit()
    conn.close()
    url = "jdbc:sqlite:{}".format(path)
    lk = lynx.LynxKite()
    cp = lk.import_jdbc(
        table="jdbc-" + str(view),
        jdbcUrl=url,
        jdbcTable="subscribers",
        keyColumn="id",
        view=view)
    res = lk.sql("select * from `cp` order by id", cp=cp)
    self.assertEqual(
        res.take(5),
        [{'gender': 'Male',
          'level': 10.0,
          'name': 'Daniel',
          'id': 1,
          'race condition': 'Halfling',
                            'n': 'A'},
         {'gender': 'Female',
          'level': 20.0,
          'name': 'Beata',
          'id': 2,
          'race condition': 'Dwarf',
                            'n': 'B'},
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
            {'id': 5}])

  def test_jdbc_import(self):
    self.stub_test_jdbc(False)

  def test_jdbc_view(self):
    self.stub_test_jdbc(True)


if __name__ == '__main__':
  unittest.main()
