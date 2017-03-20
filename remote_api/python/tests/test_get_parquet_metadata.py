import unittest
import lynx
from datetime import datetime


class TestGetParquetMetadata(unittest.TestCase):

  lk = lynx.LynxKite()
  p = lk.new_project()
  p.createVertices(size=1000000)

  def generate_view(self, limit, frame=p):
    '''Used to generate views for the tests. The first test should test if it really works.'''
    return self.lk.sql('SELECT * FROM `p` LIMIT {}'.format(limit), p=frame)

  def test_get_row_count(self):
    for row_count in [0, 1, 2, 1111, 1000000]:
      view = self.generate_view(row_count)
      path = 'DATA$/tmp/test_get_parquet_metadata_{}.parquet'.format(row_count)
      view.export_parquet(path)
      self.assertEqual(self.lk.get_parquet_metadata(path).rowCount, row_count)

if __name__ == '__main__':
  unittest.main()
