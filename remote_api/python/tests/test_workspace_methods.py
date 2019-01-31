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
  def level2(g):
    return g.sql('select 2 as level from input')

  @subworkspace
  def level3(g):
    return g.sql('select 3 as level from input')

  @lk.workspace()
  def nested_ws():
    l1 = level1()
    l2 = level2(l1)
    l3 = level3(l2)
    return l3
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

  def test_each_nested_ws_is_different(self):
    """Checks that the computation of a nested_ws doesn't have any effect on the
    next nested_ws.
    """
    ws1 = self.new_nested_ws()
    ws2 = self.new_nested_ws()
    state1 = ws1()
    state2 = ws2()
    for s in [state1, state2]:
      progress = s.get_progress()
      self.assertEqual(progress.notYetStarted, 1)
    state1.compute()
    self.assertEqual(state1.get_progress().computed, 1)
    self.assertEqual(state2.get_progress().notYetStarted, 1)

  def test_find_nested(self):
    for i in range(3, 0, -1):
      nested_ws = self.new_nested_ws()
      self.assertTrue(is_uncomputed(nested_ws()))
      table = nested_ws.recursively_find_custom_boxes_by_name(f'level{i}')[0]
      level = table.get_table_data().data[0][0].double
      self.assertEqual(level, i)
