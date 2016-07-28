import unittest
import lynx
import os
import time


class TestACL(unittest.TestCase):

  def test_acl(self):
    name = 'acl-table'
    lk = lynx.LynxKite()
    lk._request('/ajax/discardAllReallyIMeanIt')
    p = lk.new_project()
    p.save(name, 'user', 'user')
    lk.change_acl(name, '*', '*')

if __name__ == '__main__':
  unittest.main()
