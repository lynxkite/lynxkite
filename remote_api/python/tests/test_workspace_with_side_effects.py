import unittest
import lynx.kite
from lynx.kite import pp, text


class TestWorkspaceWithSideEffects(unittest.TestCase):

  def test_ws_with_side_effects(self):
    lk = lynx.kite.LynxKite()

    @lk.workspace_with_side_effect(parameters=[text('export_path')])
    def csv_exporter(sec, table):
      table.exportToCSV(path=pp('$export_path')).register(sec)

    @lk.workspace_with_side_effect('Example graph exports')
    def eg_exports(sec):
      eg = lk.createExampleGraph()
      t1 = eg.sql('select name, age from vertices where age < 10')
      t2 = eg.sql('select name, income from vertices where income > 1000')
      csv_exporter(t1, export_path='DATA$/side effect exports/a').register(sec)
      csv_exporter(t2, export_path='DATA$/side effect exports/b').register(sec)

    lk.save_workspace_recursively(eg_exports, 'side effect example folder')
    eg_exports.trigger_all_side_effects('side effect example folder')
    i1 = lk.importCSV(filename='DATA$/side effect exports/a')
    i2 = lk.importCSV(filename='DATA$/side effect exports/b')
    self.assertEqual(i1.get_table_data().data[0][0].string, 'Isolated Joe')
    self.assertEqual(i2.get_table_data().data[0][0].string, 'Bob')
