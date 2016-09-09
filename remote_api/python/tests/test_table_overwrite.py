# This module tests the exportViewToFile method with the SaveMode.OverWrite setting.
# The hadoop paths and scp addresses are correct only in the native+local+dockered version.

import unittest
import lynx.luigi
import subprocess


lk = lynx.LynxKite()

class SCPtoHDFSTask(lynx.luigi.SCPtoHDFSTask):
    def source(self):
        return 'localhost', '/root/lynx/overwrite_test_dir/file.txt'
    def destination(self):
        return 'hdfs://localhost:9000/user/root/lynxkite/overwrite_test_dir/file.txt'


class TestTable(unittest.TestCase):

  def test_hadoop_parquet_export(self):
    lk._request('/ajax/discardAllReallyIMeanIt')
    view = lk.sql('SELECT 1')
    subprocess.call(['hadoop', 'fs', '-rm', '-r', '/user/root/lynxkite/overwrite_test_table'])
    subprocess.call(['hadoop', 'fs', '-mkdir', '-p', '/user/root/lynxkite/overwrite_test_table'])
    subprocess.call(['hadoop', 'fs', '-touchz',
                     '/user/root/lynxkite/overwrite_test_table/dummy.txt'])
    view.export_parquet('DATA$/overwrite_test_table')


  def test_scp_to_hdfs(self):
    lk._request('/ajax/discardAllReallyIMeanIt')
    # Preparing scp dir and file.
    subprocess.call(['rm', '-rf', '/root/lynx/overwrite_test_dir'])
    subprocess.call(['mkdir', '-p', '/root/lynx/overwrite_test_dir'])
    subprocess.check_call(['touch', '/root/lynx/overwrite_test_dir/file.txt'])
    # Preparing hdfs dir without _SUCCESS.
    subprocess.call(['hadoop', 'fs', '-rm', '-r', '/user/root/lynxkite/overwrite_test_dir'])
    subprocess.call(['hadoop', 'fs', '-mkdir', '-p', '/user/root/lynxkite/overwrite_test_dir'])
    task = SCPtoHDFSTask()
    task.run()
    self.assertTrue(task.output().exists())


  def test_scp_to_hdfs(self):
    lk._request('/ajax/discardAllReallyIMeanIt')
    # Preparing scp dir and file.
    subprocess.call(['rm', '-rf', '/root/lynx/overwrite_test_dir'])
    subprocess.call(['mkdir', '-p', '/root/lynx/overwrite_test_dir'])
    subprocess.check_call(['touch', '/root/lynx/overwrite_test_dir/file.txt'])
    # Preparing hdfs dir without _SUCCESS.
    subprocess.call(['hadoop', 'fs', '-rm', '-r', '/user/root/lynxkite/overwrite_test_dir'])
    subprocess.call(['hadoop', 'fs', '-mkdir', '-p', '/user/root/lynxkite/overwrite_test_dir'])
    task = SCPtoHDFSTask()
    task.run()
    self.assertTrue(task.output().exists())


if __name__ == '__main__':
  unittest.main()
