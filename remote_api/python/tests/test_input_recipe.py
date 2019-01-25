import unittest
import lynx.kite
import lynx.automation
from datetime import datetime


class TestInputRecipe(unittest.TestCase):

  def test_snapshot_is_ready(self):
    lk = lynx.kite.LynxKite()
    tss = lynx.automation.TableSnapshotSequence(lk, 'test_input_recipe', '0 0 * * *')
    input_recipe = lynx.automation.TableSnapshotRecipe(tss)
    date = datetime(2018, 1, 9, 0, 0)
    tss.remove_date(date)
    self.assertEqual(input_recipe.is_ready(date), False)
    lk.createExampleGraph().sql('select name, age from vertices').save_to_sequence(tss, date)
    self.assertEqual(input_recipe.is_ready(date), True)

  def test_snapshot_input_recipe_build_boxes(self):
    lk = lynx.kite.LynxKite()
    tss = lynx.automation.TableSnapshotSequence(lk, 'test_input_recipe_boxes', '0 0 * * *')
    input_recipe = lynx.automation.TableSnapshotRecipe(tss)
    date = datetime(2018, 1, 10, 0, 0)
    tss.remove_date(date)
    lk.createExampleGraph().sql('select age from vertices').save_to_sequence(tss, date)
    input_boxes = input_recipe.build_boxes(date)
    table = input_boxes.sql('select sum(age) as sum_age from input').get_table_data()
    self.assertEqual(table.data[0][0].string, '90.8')
