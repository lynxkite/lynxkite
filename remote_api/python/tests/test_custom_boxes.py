import unittest
import random
from lynx.kite import LynxKite, subworkspace, ws_param, pp, text


lk = LynxKite()


def create_nested_custom_box():
  @subworkspace
  def identity(x):
    return x

  @subworkspace
  def inside(x):
    return identity(x)

  @lk.workspace()
  def outside():
    return inside(lk.createExampleGraph())

  return outside()


class TestTruncate(unittest.TestCase):

  def test_truncate(self):
    nested_cb = create_nested_custom_box()
    t = nested_cb.truncate('identity')
    print(t.sql('select * from vertices').df())
