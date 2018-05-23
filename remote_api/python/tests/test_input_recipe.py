import unittest
import lynx.kite
import lynx.automation
from datetime import datetime


class TestInputRecipe(unittest.TestCase):

  def test_snapshot_is_ready(self):
    lk = lynx.kite.LynxKite()
    tss = lynx.kite.TableSnapshotSequence('test_input_recipe', '0 0 * * *', lk)
    input_recipe = lynx.automation.TableSnapshotRecipe(tss)
    date = datetime(2018, 1, 9, 0, 0)
    lk.remove_name(tss.snapshot_name(date), force=True)
    self.assertEqual(input_recipe.is_ready(lk, date), False)
    lk.createExampleGraph().sql('select name, age from vertices').save_to_sequence(tss, date)
    self.assertEqual(input_recipe.is_ready(lk, date), True)

  def test_snapshot_input_recipe_build_boxes(self):
    lk = lynx.kite.LynxKite()
    tss = lynx.kite.TableSnapshotSequence('test_input_recipe_boxes', '0 0 * * *', lk)
    input_recipe = lynx.automation.TableSnapshotRecipe(tss)
    date = datetime(2018, 1, 10, 0, 0)
    lk.remove_name(tss.snapshot_name(date), force=True)
    lk.createExampleGraph().sql('select age from vertices').save_to_sequence(tss, date)
    input_boxes = input_recipe.build_boxes(lk, date)
    table = input_boxes.sql('select sum(age) as sum_age from input').get_table_data()
    self.assertEqual(table.data[0][0].string, '90.8')
