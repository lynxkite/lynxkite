import unittest
import lynx.kite
import lynx.automation
from lynx.automation import Schedule, utc_dt
from datetime import datetime


class TestInputRecipe(unittest.TestCase):

  def test_snapshot_is_ready(self):
    lk = lynx.kite.LynxKite()
    schedule = Schedule(utc_dt(2018, 1, 1), '0 0 * * *')
    tss = lynx.automation.TableSnapshotSequence(lk, 'test_input_recipe', schedule)
    input_recipe = lynx.automation.TableSnapshotRecipe(tss)
    date = utc_dt(2018, 1, 9, 0, 0)
    tss.remove_date(date)
    self.assertEqual(input_recipe.is_ready(date), False)
    lk.createExampleGraph().sql('select name, age from vertices').save_to_sequence(tss, date)
    self.assertEqual(input_recipe.is_ready(date), True)
    input_recipe = lynx.automation.TableSnapshotRecipe(tss, number_of_snapshots=3)
    middle_date = utc_dt(2018, 1, 8, 0, 0)
    from_date = utc_dt(2018, 1, 7, 0, 0)
    tss.remove_date(middle_date)
    tss.remove_date(from_date)
    self.assertEqual(input_recipe.is_ready(date), False)
    lk.createExampleGraph().sql('select age from vertices').save_to_sequence(tss, from_date)
    self.assertEqual(input_recipe.is_ready(date), False)
    lk.createExampleGraph().sql('select age from vertices').save_to_sequence(tss, middle_date)
    self.assertEqual(input_recipe.is_ready(date), True)

  def test_snapshot_input_recipe_build_boxes(self):
    lk = lynx.kite.LynxKite()
    schedule = Schedule(utc_dt(2018, 1, 1), '0 0 * * *')
    tss = lynx.automation.TableSnapshotSequence(lk, 'test_input_recipe_boxes', schedule)
    input_recipe = lynx.automation.TableSnapshotRecipe(tss)
    date = utc_dt(2018, 1, 10, 0, 0)
    tss.remove_date(date)
    lk.createExampleGraph().sql('select age from vertices').save_to_sequence(tss, date)
    input_boxes = input_recipe.build_boxes(date)
    table = input_boxes.sql('select sum(age) as sum_age from input').get_table_data()
    self.assertEqual(table.data[0][0].string, '90.8')
