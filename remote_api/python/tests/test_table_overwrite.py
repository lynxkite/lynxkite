import unittest
import lynx.luigi
import subprocess


lk = lynx.LynxKite()

class TestTable(unittest.TestCase):

    def test_run1(self):
        lk._request('/ajax/discardAllReallyIMeanIt')
        view = lk.sql('SELECT 1')
        subprocess.call(['hadoop', 'fs', '-rm', '-r', '/user/root/lynxkite/overwrite_test_table'])
        subprocess.call(['hadoop', 'fs', '-mkdir', '-p', '/user/root/lynxkite/overwrite_test_table'])
        subprocess.call(['hadoop', 'fs', '-touchz', '/user/root/lynxkite/overwrite_test_table/dummy.txt'])
        view.export_parquet('DATA$/overwrite_test_table')



if __name__ == '__main__':
    unittest.main()
