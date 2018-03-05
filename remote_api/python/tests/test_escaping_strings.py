import unittest
import lynx.kite
import shlex


class TestEscapingStrings(unittest.TestCase):

  def name_of_test_data(self):
    import os
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return dir_path + '/strings.csv'

  def import_test_data(self):
    lk = lynx.kite.LynxKite()
    with open(self.name_of_test_data()) as f:
      csv_path = lk.upload(f)
    return lk.importCSV(filename=csv_path)

  def test_reading_strings_to_dataframe(self):
    print(self.import_test_data().df())

  def test_escape_on_test_data(self):
    with open(self.name_of_test_data()) as f:
      for line in f:
        print(lynx.kite.escape(line))

  def test_escaped_query(self):
    lk = lynx.kite.LynxKite()
    query = {}
    box = {}
    with open(self.name_of_test_data()) as f:
      for i, line in enumerate(f):
        # No coma in raw data
        test_id, test_name, test_raw = line.split(',')
        query[i] = '''
          select {} as test_id,
          '{}' as test_name,
          {} as raw_value
          from vertices
          '''.format(test_id, test_name, lynx.kite.escape(test_raw))
        box[i] = lk.createExampleGraph().sql(query[i])
    print(box)
    # At most 10 test cases because of the SQL box limit.
    table_names = {
        1: 'one', 2: 'two', 3: 'three', 4: 'four', 5: 'five',
        6: 'six', 7: 'seven', 8: 'eight'}
    boxes = [box[i] for i in range(1, 9)]
    result_query = ' union all '.join(
        ['select * from {}'.format(table_names[i]) for i in range(1, 9)])
    print(lk.sql(result_query, *boxes).df())
