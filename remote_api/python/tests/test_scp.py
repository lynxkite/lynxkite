import unittest
import luigi
import lynx.luigi
from unittest import mock
import types
import os
import shutil


class TestInput(luigi.ExternalTask):

  def output(self): return lynx.luigi.HDFSTarget('input')


class TestTask(lynx.luigi.TransferTask):

  def requires(self): return TestInput()

  def output(self): return lynx.luigi.SCPTarget('dsthost', 'dstpath')


class TestSCP(unittest.TestCase):

  @mock.patch('subprocess.check_call')
  def test_run(self, check_call):
    local_path = ''

    def hdsf_download(cmd, **kwargs):
      nonlocal local_path
      if cmd[:-1] == ['hadoop', 'fs', '-cp', 'input' + '/*']:
        local_path = cmd[-1].split(':')[1]
        with open(os.path.join(local_path, 'one'), 'w') as f:
          f.write('one')
        with open(os.path.join(local_path, 'two'), 'w') as f:
          f.write('two')
    check_call.side_effect = hdsf_download

    t = TestTask()

    def transform(src, dst):
      for name in os.listdir(src):
        shutil.copyfile(os.path.join(src, name), os.path.join(dst, name))

    with mock.patch.object(t, 'transform_fn') as m:
      m.side_effect = transform
      t.run()
      self.assertEqual(1, t.transform_fn.call_count)

    check_call.assert_any_call(['hadoop', 'fs', '-cp', 'input/*',
                                'file:' + local_path], env=mock.ANY)
    check_call.assert_any_call(['ssh', 'dsthost', "mkdir -p 'dstpath'"])
    check_call.assert_any_call(['ssh', 'dsthost', "cat > 'dstpath/one'"], stdin=mock.ANY)
    check_call.assert_any_call(['ssh', 'dsthost', "cat > 'dstpath/two'"], stdin=mock.ANY)
    check_call.assert_any_call(['ssh', 'dsthost', "cat > 'dstpath/_SUCCESS'"], stdin=mock.ANY)
    self.assertEqual(5, check_call.call_count)

if __name__ == '__main__':
  unittest.main()
