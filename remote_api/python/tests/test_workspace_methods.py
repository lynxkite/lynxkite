import unittest
from lynx.kite import LynxKite, subworkspace


lk = LynxKite()


def create_nested_workspace(seed):
  """The outputs are the same regardless of the seed, but using different seeds ensures
  that it's a different LK entity and thus enforce calculation.
  """
  start = lk.createVertices(size=seed)

  @subworkspace
  def level1():
    return start.sql('select 1 as level from vertices')

  @subworkspace
  def level2():
    l1 = level1()
    return l1.sql('select 2 as level from input')

  @subworkspace
  def level3():
    l2 = level2()
    return l2.sql('select 3 as level from input')

  @lk.workspace()
  def nested_ws():
    return level3()
  return nested_ws


def is_uncomputed(state):
  return state.get_progress().notYetStarted


class TestRecursivelyFindCustomBoxesByName(unittest.TestCase):
  seed = 1

  def new_nested_ws(self):
    """Returns a new nested_ws on each access. Makes sure that the returned new_nested_ws is
    not computed yet."""
    uncomputed_found = False
    while(not uncomputed_found):
      ws = create_nested_workspace(self.seed)
      self.seed += 1
      uncomputed_found = is_uncomputed(ws())
    return ws

  def test_find_nested(self):
    for i in range(3, 0, -1):
      nested_ws = self.new_nested_ws()
      table = nested_ws.recursively_find_custom_boxes_by_name(f'level{i}')[0]
      self.assertTrue(is_uncomputed(table))
      level = table.get_table_data().data[0][0].double
      self.assertEqual(level, i)
