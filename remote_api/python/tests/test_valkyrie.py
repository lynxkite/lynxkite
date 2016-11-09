import datetime
import unittest
from lynx.luigi import valkyrie
from unittest import mock
import types


class TestValkyrie(unittest.TestCase):

  def test_ttl(self):
    ttl = valkyrie.ValkyrieCleanup._ttl('root/some directory (ttl=48h)')
    self.assertEqual(datetime.timedelta(days=2), ttl)

  @mock.patch('lynx.util.HDFS.rm')
  @mock.patch('lynx.util.HDFS.list')
  def test_run(self, hdfs_list, hdfs_rm):
    SN = types.SimpleNamespace
    hdfs_list.return_value = [
        SN(datetime=datetime.datetime(2016, 8, 1, 15, 32), path='old, no ttl'),
        SN(datetime=datetime.datetime(2016, 8, 1, 15, 32), path='old, long ttl (ttl=7d)'),
        SN(datetime=datetime.datetime(2016, 8, 12, 9, 22), path='new, long ttl (ttl=7d)'),
        SN(datetime=datetime.datetime(2016, 8, 12, 9, 22), path='new, short ttl (ttl=1h)'),
    ]
    v = valkyrie.ValkyrieCleanup(date=datetime.datetime(2016, 8, 12, 12, 5, 0))
    output = mock.MagicMock()
    with mock.patch.multiple(v, output=mock.DEFAULT, lk=mock.DEFAULT) as mocks:
      mocks['output'].return_value = output
      v.run()
    # Assert the right files are deleted.
    hdfs_rm.assert_any_call('old, long ttl (ttl=7d)')
    hdfs_rm.assert_any_call('new, short ttl (ttl=1h)')
    # Assert no other files are deleted.
    self.assertEqual(4, hdfs_rm.call_count)
    # Assert the marker file is created.
    output.open.assert_called_once_with('w')


if __name__ == '__main__':
  unittest.main()
