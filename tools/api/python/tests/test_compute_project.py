import unittest
import lynx
import os


class TestComputeProject(unittest.TestCase):
  """
  All import calls use the same backend function,
  no need to test them separately.
  """

  def test_force(self):
    p = lynx.Project()
    p.newVertexSet(size = 5000)
    p.computeProject()

if __name__ == '__main__':
  unittest.main()
