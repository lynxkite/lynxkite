import datetime
import unittest
import lynx.luigi
from unittest import mock
import types


class TestTask(lynx.luigi.SCPTask):

  def destination(self): return 'dsthost', 'dstpath'


class TestSCP(unittest.TestCase):

  @mock.patch('subprocess.check_call')
  @mock.patch('lynx.util.HDFS.list')
  def test_run(self, hdfs_list, check_call):
    SN = types.SimpleNamespace
    hdfs_list.return_value = [SN(path='input/_SUCCESS'), SN(path='input/one'), SN(path='input/two')]
    t = TestTask()
    with mock.patch.object(t, 'input') as inp:
      inp().hdfs_path.return_value = 'input'
      t.run()
    check_call.assert_any_call(['ssh', 'dsthost', "mkdir -p 'dstpath'"])
    check_call.assert_any_call(
        'hadoop fs -cat \'input/one\' | ssh dsthost "cat > \'dstpath/one\'"', shell=True)
    check_call.assert_any_call(
        'hadoop fs -cat \'input/two\' | ssh dsthost "cat > \'dstpath/two\'"', shell=True)
    check_call.assert_any_call(
        'hadoop fs -cat \'input/_SUCCESS\' | ssh dsthost "cat > \'dstpath/_SUCCESS\'"', shell=True)
    self.assertEqual(4, check_call.call_count)


if __name__ == '__main__':
  unittest.main()
