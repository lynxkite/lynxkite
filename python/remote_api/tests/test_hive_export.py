import unittest
import lynx.kite


class TestHive(unittest.TestCase):
  def test_hive_overwrite_with_partitions(self):
    lk = lynx.kite.LynxKite()
    table = lk.createExampleGraph().sql('select * from `vertices`')
    with self.assertRaises(Exception) as context:
      table.exportToHiveNow(
          table='out',
          mode='Drop the table if it already exists',
          partition_by="name,gender")
    print('hello')
    print(str(context.exception))
    print('over')
    self.assertTrue(
        'overwrite mode cannot be used with partition columns' in str(
            context.exception))
