import unittest
import lynx
from datetime import datetime


class TestViewSchema(unittest.TestCase):

  lk = lynx.LynxKite()

  def get_column_names(self, schema_response):
    return list(map(lambda x: x.name, schema_response.schema))

  def get_data_types(self, schema_response):
    return list(map(lambda x: x.dataType, schema_response.schema))

  def test_schema(self):
    '''Used to generate a view and check its schema.'''
    schema_response = self.lk.sql(
        '''SELECT false AS zero, 1 AS one, 2.0 AS two, 'three' AS three''').schema()
    self.assertEqual(self.get_column_names(schema_response), ['zero', 'one', 'two', 'three'])
    self.assertEqual(
        self.get_data_types(schema_response), [
            'BooleanType', 'IntegerType', 'DoubleType', 'StringType'])


if __name__ == '__main__':
  unittest.main()
