import unittest
import os
import shutil
from datetime import datetime, timedelta
import lynx.kite as kite
import lynx.automation as aut
from lynx.automation import Schedule, utc_dt


lk = kite.LynxKite()


class TestRecipe(aut.InputRecipe):
  def __init__(self):
    self.called = 0

  def is_ready(self, date):
    return True

  def build_boxes(self, date):
    self.called += 1
    return lk.createExampleGraph().sql(f'select {self.called} as num from vertices')

  def validate(self, date):
    pass


@lk.workspace()
def identity(state):
  return {'state': state}


test_date = utc_dt(2018, 6, 26)
day_before = test_date - timedelta(days=1)
utc_test_date = utc_dt(2018, 6, 26)


class TestTaskIdempotence(unittest.TestCase):

  def test_input_recipe(self):
    recipe = TestRecipe()
    dag = aut.WorkspaceSequence(identity, schedule=Schedule(day_before, '0 0 * * *'),
                                lk_root='input_idempotence', input_recipes=[recipe]).to_dag()
    input_task = [t for t in dag if isinstance(t, aut.Input)][0]
    lk.remove_name('input_idempotence/input-snapshots', force=True)
    input_task.run(utc_test_date)
    self.assertEqual(recipe.called, 1)
    input_task.run(utc_test_date)
    self.assertEqual(recipe.called, 1)
    lk.remove_name('input_idempotence/input-snapshots', force=True)
    input_task.run(utc_test_date)
    self.assertEqual(recipe.called, 2)

  def test_output(self):
    wss = aut.WorkspaceSequence(identity, schedule=Schedule(day_before, '0 0 * * *'),
                                lk_root='output_idempotence', input_recipes=[TestRecipe()])
    dag = wss.to_dag()
    lk.remove_name('output_idempotence/input-snapshots', force=True)
    for t in dag:
      t.run(utc_test_date)
    lk.remove_name('output_idempotence/input-snapshots', force=True)
    for t in dag:
      t.run(utc_test_date)

    output = list(wss.output_sequences.values())[0].read_date(utc_test_date)
    first_cell = output.get_table_data().data[0][0].double
    self.assertAlmostEqual(first_cell, 2)

  @unittest.skip('Export to CSV is not idempotent yet')
  def test_export(self):
    prefixed_path = 'DATA$/tmp/test-export-csv'
    resolved_path = lk.get_prefixed_path(prefixed_path).resolved[len('file:'):]
    shutil.rmtree(resolved_path, ignore_errors=True)

    @lk.workspace_with_side_effects()
    def exporter(sec, state):
      state.exportToCSV(path=prefixed_path).register(sec)
      return {'state': state}

    dag = aut.WorkspaceSequence(exporter, schedule=Schedule(day_before, '0 0 * * *'),
                                lk_root='export_idempotence', input_recipes=[TestRecipe()]).to_dag()
    lk.remove_name('export_idempotence/input-snapshots', force=True)
    for t in dag:
      t.run(utc_test_date)
    creation_time = os.path.getmtime(resolved_path)
    for t in dag:
      t.run(utc_test_date)
    self.assertEqual(os.path.getmtime(resolved_path), creation_time)

    # removing the input forces the recipe to create a different input on next run
    lk.remove_name('export_idempotence/input-snapshots', force=True)
    for t in dag:
      t.run(utc_test_date)
    self.assertLess(creation_time, os.path.getmtime(resolved_path))
