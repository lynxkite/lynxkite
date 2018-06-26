import unittest
from datetime import datetime, timedelta
import lynx.kite as kite
import lynx.automation as aut


lk = kite.LynxKite()


class TestRecipe(aut.InputRecipe):
  def __init__(self):
    self.called = 0

  def is_ready(self, date):
    return True

  def build_boxes(self, date):
    self.called += 1
    return lk.createExampleGraph()

  def validate(self, date):
    pass


class TestTaskIdempotence(unittest.TestCase):

  def test_input_recipe(self):
    @lk.workspace()
    def identity(state):
      return {'state': state}

    test_date = datetime(2018, 6, 26)
    day_before = test_date - timedelta(days=1)
    recipe = TestRecipe()
    wss = aut.WorkspaceSequence(identity, schedule='0 0 * * *', start_date=day_before,
                                lk_root='input_idempotence', input_recipes=[recipe],
                                params={}, dfs_root='')
    dag = wss.to_dag()
    input_task = [t for t in dag if isinstance(t, aut.Input)][0]
    input_task.run(test_date)
    self.assertEqual(recipe.called, 1)
    input_task.run(test_date)
    self.assertEqual(recipe.called, 1)
    lk.remove_name('input_idempotence/input-snapshots', force=True)
    input_task.run(test_date)
    self.assertEqual(recipe.called, 2)
