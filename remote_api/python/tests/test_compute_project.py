import unittest
import lynx
import os


class TestComputeProject(unittest.TestCase):

  def test_force(self):
    p = lynx.Project()
    p.newVertexSet(size = 5000)
    p.compute()

if __name__ == '__main__':
  unittest.main()
