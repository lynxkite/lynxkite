import unittest
import lynx.kite


class TestEscapingStrings(unittest.TestCase):

  def name_of_test_data(self):
    import os
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return dir_path + '/strings.csv'

  def import_test_data(self):
    lk = lynx.kite.LynxKite()
    with open(self.name_of_test_data()) as f:
      csv_path = lk.upload(f)
    return lk.importCSV(filename=csv_path, columns='test_name,raw_value')

  def test_escaped_query(self):
    lk = lynx.kite.LynxKite()
    query = {}
    box = {}
    import csv
    with open(self.name_of_test_data()) as f:
      csv_reader = csv.reader(f, delimiter=',')
      for i, row in enumerate(csv_reader):
        test_name, test_raw = row[:2]
        query[i] = '''select
          '{}' as test_name,
          '{}' as raw_value
          '''.format(test_name, lynx.kite.escape(test_raw))
    result_query = ' union all '.join([q for key, q in query.items()])

    original = self.import_test_data().df()
    escaped = lk.createExampleGraph().sql(result_query).df()
    print(original.equals(escaped))
