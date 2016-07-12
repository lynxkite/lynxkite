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
    (NULL, 4, NULL, NULL, NULL, NULL);
    """)
    conn.commit()
    conn.close()

    url = "jdbc:sqlite:{}".format(path)
    p = lynx.Project()
    p.import_jdbc(table="jdbc-" + str(view), jdbcUrl=url, jdbcTable="subscribers", keyColumn="id", view=view)

  def test_jdbc_import(self):
    self.stub_test_jdbc(False)

  def test_jdbc_view(self):
    self.stub_test_jdbc(True)

if __name__ == '__main__':
  unittest.main()