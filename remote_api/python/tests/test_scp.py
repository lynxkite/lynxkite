import unittest
import luigi
import lynx.luigi
from unittest import mock
import types


class TestInput(luigi.ExternalTask):

  def output(self): return lynx.luigi.HDFSTarget('input')


class TestTask(lynx.luigi.TransferTask):

  def requires(self): return TestInput()

  def output(self): return lynx.luigi.SCPTarget('dsthost', 'dstpath')


class TestSCP(unittest.TestCase):

  @mock.patch('subprocess.check_call')
  @mock.patch('subprocess.Popen')
  @mock.patch('lynx.util.HDFS.list')
  def test_run(self, hdfs_list, popen, check_call):
    SN = types.SimpleNamespace
    hdfs_list.return_value = [SN(path='input/_SUCCESS'), SN(path='input/one'), SN(path='input/two')]
    t = TestTask()
    t.run()
    check_call.assert_any_call(['ssh', 'dsthost', "mkdir -p 'dstpath'"])
    popen.assert_any_call(['hadoop', 'fs', '-cat', 'input/one'], env=mock.ANY, stdout=mock.ANY)
    check_call.assert_any_call(['ssh', 'dsthost', "cat > 'dstpath/one'"], stdin=mock.ANY)
    popen.assert_any_call(['hadoop', 'fs', '-cat', 'input/two'], env=mock.ANY, stdout=mock.ANY)
    check_call.assert_any_call(['ssh', 'dsthost', "cat > 'dstpath/two'"], stdin=mock.ANY)
    popen.assert_any_call(['hadoop', 'fs', '-cat', 'input/_SUCCESS'], env=mock.ANY, stdout=mock.ANY)
    check_call.assert_any_call(['ssh', 'dsthost', "cat > 'dstpath/_SUCCESS'"], stdin=mock.ANY)
    self.assertEqual(3, popen.call_count)
    self.assertEqual(4, check_call.call_count)

if __name__ == '__main__':
  unittest.main()
