import datetime
from lynx import util
import types
import unittest
from unittest import mock


class TestUtil(unittest.TestCase):

  def test_parse_duration(self):
    d = util.parse_duration('1d')
    self.assertEqual(datetime.timedelta(days=1), d)
    d = util.parse_duration('10h')
    self.assertEqual(datetime.timedelta(hours=10), d)
    d = util.parse_duration('30m')
    self.assertEqual(datetime.timedelta(minutes=30), d)
    d = util.parse_duration('1d10h30m')
    self.assertEqual(datetime.timedelta(days=1, hours=10, minutes=30), d)
    with self.assertRaises(AssertionError):
      d = util.parse_duration('')
    with self.assertRaises(AssertionError):
      d = util.parse_duration('hello')
    with self.assertRaises(AssertionError):
      d = util.parse_duration('1.5d')
    with self.assertRaises(AssertionError):
      d = util.parse_duration('-30m')
    with self.assertRaises(AssertionError):
      d = util.parse_duration('2h ')
    with self.assertRaises(AssertionError):
      d = util.parse_duration('0d')

  def test_ttl(self):
    ttl = util.get_ttl_from_path('root/some directory (ttl=48h)')
    self.assertEqual(datetime.timedelta(days=2), ttl)


class TestHDFS(unittest.TestCase):

  @mock.patch('subprocess.check_output')
  def test_list(self, check_output):
    check_output.return_value = '''
Found 3 items
-rw-r--r--   1 hadoop hadoop        393 2016-07-27 15:32 hdfs://ip-10-165-135-220:8020/user/hadoop/x/README.md
-rw-r--r-- + 1 hadoop hadoop          0 2016-07-27 15:32 hdfs://ip-10-165-135-220:8020/user/hadoop/x/_SUCCESS
-rw-r--r--   1 hadoop hadoop         16 2016-07-27 15:33 hdfs://ip-10-165-135-220:8020/user/hadoop/x/hello world
    '''.encode('utf-8').strip()
    ls = util.HDFS.list('mydir')
    check_output.assert_called_once_with(['hadoop', 'fs', '-ls', 'mydir'], env=None)
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
    ls = util.HDFS.rm('my dir')
    check_call.assert_called_once_with(['hadoop', 'fs', '-rm', '-r', '-skipTrash', 'my dir'],
                                       env=None)
    # Check that special characters are escaped.
    check_call.reset_mock()
    ls = util.HDFS.rm('my \\ dir [ttl=7d]')
    check_call.assert_called_once_with(
        ['hadoop', 'fs', '-rm', '-r', '-skipTrash', 'my \\\\ dir \\[ttl=7d\\]'], env=None)


if __name__ == '__main__':
  unittest.main()
