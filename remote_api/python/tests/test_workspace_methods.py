import unittest
import random
from lynx.kite import LynxKite, subworkspace, ws_param, pp, text


lk = LynxKite()


def get_one(seed):
  return lk.createVertices(size=seed).sql('select 1 as number, count(*) from vertices')


def create_nested_workspace(seed):
  """The outputs are the same regardless of the seed, but using different seeds ensures
  that it's a different LK entity and thus enforce calculation.
  """
  one = get_one(seed)

  @ws_param('number')
  @subworkspace
  def add_number(table):
    return table.sql(pp('select number + ${number} as number from input'))

  @subworkspace
  def four():
    two = add_number(one, number=pp('1'))
    four = add_number(two, number=pp('${2}'))  # Also try out parametric parameters.
    return four

  @subworkspace
  def five():
    f = four()
    return add_number(f, number='1')

  @lk.workspace()
  def six():
    f = five()
    return add_number(f, number='1')

  return six


def is_uncomputed(state):
  return state.get_progress().notYetStarted


class TestRecursivelyFindCustomBoxesByName(unittest.TestCase):
  seed = 0

  def new_nested_ws(self):
    """Returns a new nested_ws on each access.

    Makes sure that no part of it is computed yet by checking that its base is not calculated yet.
    """
    found_uncomputed = False
    while(not found_uncomputed):
      # Randomize to make it easier to find an uncomputed one.
      self.seed += random.randint(1, 1000)
      one = get_one(self.seed)
      found_uncomputed = is_uncomputed(one)
    ws = create_nested_workspace(self.seed)
    return ws

  def test_find_nested_subworkspace(self):
    nested_ws = self.new_nested_ws()
    table4 = nested_ws.recursively_find_custom_boxes_by_name('four')[0]
    self.assertTrue(is_uncomputed(table4))
    number = table4.get_table_data().data[0][0].double
    self.assertEqual(number, 4)

  def test_find_nested_parametric_subworkspaces(self):
    nested_ws = self.new_nested_ws()
    additions = nested_ws.recursively_find_custom_boxes_by_name('add_number')
    # Make sure that none of them are computed yet.
    additions_are_uncomputed = [is_uncomputed(a) for a in additions]
    self.assertListEqual(additions_are_uncomputed, [True] * 4)
    addition_results = [a.get_table_data().data[0][0].double for a in additions]
    expected = [2, 4, 5, 6]
    self.assertListEqual(sorted(addition_results), expected)
