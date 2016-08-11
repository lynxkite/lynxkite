import datetime
import types
import unittest
from lynx.luigi import valkyrie
from unittest import mock


class TestValkyrie(unittest.TestCase):

  def test_ttl(self):
    ttl = valkyrie.ValkyrieCleanup._ttl('root/some directory [ttl=48h]')
    self.assertEqual(datetime.timedelta(days=2), ttl)

  @mock.patch('subprocess.check_output')
  def test_list(self, check_output):
    check_output.return_value = '''
Found 3 items
-rw-r--r--   1 hadoop hadoop        393 2016-07-27 15:32 hdfs://ip-10-165-135-220:8020/user/hadoop/x/README.md
-rw-r--r-- + 1 hadoop hadoop          0 2016-07-27 15:32 hdfs://ip-10-165-135-220:8020/user/hadoop/x/_SUCCESS
-rw-r--r--   1 hadoop hadoop         16 2016-07-27 15:33 hdfs://ip-10-165-135-220:8020/user/hadoop/x/hello world
    '''.encode('utf-8').strip()
    ls = valkyrie.ValkyrieCleanup._list('mydir')
    check_output.assert_called_once_with(['hadoop', 'fs', '-ls', 'mydir'])
    expected = [
        types.SimpleNamespace(
            permission='-rw-r--r--', acl=' ', replication='1',
            group='hadoop', owner='hadoop',
            size='393',
            date='2016-07-27', time='15:32', datetime=datetime.datetime(2016, 7, 27, 15, 32),
            path='hdfs://ip-10-165-135-220:8020/user/hadoop/x/README.md'),
        types.SimpleNamespace(
            permission='-rw-r--r--', acl='+', replication='1',
            group='hadoop', owner='hadoop',
            size='0',
            date='2016-07-27', time='15:32', datetime=datetime.datetime(2016, 7, 27, 15, 32),
            path='hdfs://ip-10-165-135-220:8020/user/hadoop/x/_SUCCESS'),
        types.SimpleNamespace(
            permission='-rw-r--r--', acl=' ', replication='1',
            group='hadoop', owner='hadoop',
            size='16',
            date='2016-07-27', time='15:33', datetime=datetime.datetime(2016, 7, 27, 15, 33),
            path='hdfs://ip-10-165-135-220:8020/user/hadoop/x/hello world'),
    ]
    self.assertEqual(expected, ls)

  @mock.patch('subprocess.check_call')
  def test_rm(self, check_call):
    ls = valkyrie.ValkyrieCleanup._rm('my dir')
    check_call.assert_called_once_with(['hadoop', 'fs', '-rm', '-r', 'my dir'])

  @mock.patch('subprocess.check_output')
  @mock.patch('subprocess.check_call')
  def test_run(self, check_call, check_output):
    check_output.return_value = '''
Found 3 items
-rw-r--r--   1 hadoop hadoop        393 2016-08-01 15:32 no ttl
-rw-r--r--   1 hadoop hadoop         16 2016-08-01 15:32 old, long ttl [ttl=7d]
-rw-r--r-- + 1 hadoop hadoop          0 2016-08-12 09:22 new, long ttl [ttl=7d]
-rw-r--r-- + 1 hadoop hadoop          0 2016-08-12 09:22 new, short ttl [ttl=1h]
    '''.encode('utf-8').strip()
    v = valkyrie.ValkyrieCleanup(date=datetime.datetime(2016, 8, 12, 12, 5, 0))
    output = mock.MagicMock()
    with mock.patch.multiple(v, output=mock.DEFAULT, lk=mock.DEFAULT) as mocks:
      mocks['output'].return_value = output
      v.run()
    # Assert the right files are deleted.
    check_call.assert_any_call(['hadoop', 'fs', '-rm', '-r', 'old, long ttl [ttl=7d]'])
    check_call.assert_any_call(['hadoop', 'fs', '-rm', '-r', 'new, short ttl [ttl=1h]'])
    # Assert no other files are deleted.
    self.assertEqual(2, check_call.call_count)
    # Assert the marker file is created.
    output.open.assert_called_once_with('w')


if __name__ == '__main__':
  unittest.main()
