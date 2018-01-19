import unittest
import lynx.kite
from datetime import datetime


class TestInputRecipe(unittest.TestCase):

  def test_snapshot_is_ready(self):
    lk = lynx.kite.LynxKite()
    tss = lynx.kite.TableSnapshotSequence('test_input_recipe', '0 0 * * *')
    input_recipe = lynx.kite.TableSnapshotRecipe(tss)
    state = lk.createExampleGraph().sql('select name, age from vertices')
    table_state = lk.get_state_id(state)
    date = datetime(2018, 1, 9, 0, 0)
    lk.remove_name(tss.snapshot_name(date), force=True)
    self.assertEqual(input_recipe.is_ready(lk, date), False)
    tss.save_to_sequence(lk, table_state, date)
    self.assertEqual(input_recipe.is_ready(lk, date), True)

  def test_snapshot_input_recipe_build_boxes(self):
    lk = lynx.kite.LynxKite()
    tss = lynx.kite.TableSnapshotSequence('test_input_recipe_boxes', '0 0 * * *')
    input_recipe = lynx.kite.TableSnapshotRecipe(tss)
    state = lk.createExampleGraph().sql('select age from vertices')
    table_state = lk.get_state_id(state)
    date = datetime(2018, 1, 10, 0, 0)
    lk.remove_name(tss.snapshot_name(date), force=True)
    tss.save_to_sequence(lk, table_state, date)
    input_boxes = input_recipe.build_boxes(lk, date)
    table = input_boxes.sql('select sum(age) as sum_age from input').get_table_sample(lk)
    self.assertEqual(table.data[0][0].string, '90.8')
