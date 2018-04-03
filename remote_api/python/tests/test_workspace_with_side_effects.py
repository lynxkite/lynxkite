import unittest
import lynx.kite
from lynx.kite import pp, text


class TestWorkspaceWithSideEffects(unittest.TestCase):

  def test_ws_with_side_effects(self):
    lk = lynx.kite.LynxKite()

    @lk.workspace(parameters=[text('export_path')], with_side_effects=True)
    def csv_exporter(sec, table):
      table.exportToCSV(path=pp('$export_path')).register(sec)

    @lk.workspace('Example graph exports', with_side_effects=True)
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

  def test_side_effects_from_different_boxes(self):
    lk = lynx.kite.LynxKite()

    @lk.workspace(parameters=[text('snapshot_path')], with_side_effects=True)
    def save_graph_to_snapshot(sec, graph):
      graph.sql('select * from vertices').saveToSnapshot(path=pp('$snapshot_path')).register(sec)

    @lk.workspace(parameters=[text('snapshot_path')], with_side_effects=True)
    def save_and_return_graph(sec, graph):
      graph.sql('select * from vertices').saveToSnapshot(path=pp('$snapshot_path')).register(sec)
      return dict(graph=graph)

    @lk.workspace('Muliple graph snapshots', with_side_effects=True)
    def eg_snapshots(sec):
      eg = lk.createExampleGraph()
      save_graph_to_snapshot(eg, snapshot_path='side effect snapshots/a').register(sec)
      first = save_and_return_graph(eg, snapshot_path='side effect snapshots/b')
      first.register(sec)
      save_graph_to_snapshot(first, snapshot_path='side effect snapshots/c').register(sec)

    lk.save_workspace_recursively(eg_snapshots, 'side effect snapshots example folder')
    lk.remove_name('side effect snapshots', force=True)
    eg_snapshots.trigger_all_side_effects('side effect snapshots example folder')
    entries = lk.list_dir('side effect snapshots')
    expected = [
        'side effect snapshots/a',
        'side effect snapshots/b',
        'side effect snapshots/c']
    self.assertEqual([e.name for e in entries], expected)

  def test_side_effects_unsaved_workspaces(self):
    lk = lynx.kite.LynxKite()

    @lk.workspace(parameters=[text('snapshot_path')], with_side_effects=True)
    def save_graph_to_snapshot(sec, graph):
      graph.sql('select * from vertices').saveToSnapshot(path=pp('$snapshot_path')).register(sec)

    @lk.workspace(with_side_effects=True)
    def eg_snapshots(sec):
      eg = lk.createExampleGraph()
      save_graph_to_snapshot(eg, snapshot_path='unsaved/a').register(sec)
      save_graph_to_snapshot(eg, snapshot_path='unsaved/b').register(sec)

    lk.remove_name('unsaved', force=True)
    eg_snapshots.trigger_all_side_effects()
    entries = lk.list_dir('unsaved')
    expected = ['unsaved/a', 'unsaved/b']
    self.assertEqual([e.name for e in entries], expected)
