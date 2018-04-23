import unittest
import lynx.kite
from lynx.kite import pp, text


class TestWorkspaceWithSideEffects(unittest.TestCase):

  def test_ws_with_side_effects(self):
    lk = lynx.kite.LynxKite()

    @lk.workspace_with_side_effects(parameters=[text('export_path')])
    def csv_exporter(sec, table):
      table.exportToCSV(path=pp('$export_path')).register(sec)

    @lk.workspace_with_side_effects('Example graph exports')
    def eg_exports(sec):
      eg = lk.createExampleGraph()
      t1 = eg.sql('select name, age from vertices where age < 10')
      t2 = eg.sql('select name, income from vertices where income > 1000')
      csv_exporter(t1, export_path='DATA$/side effect exports/a').register(sec)
      csv_exporter(t2, export_path='DATA$/side effect exports/b').register(sec)

    eg_exports.save('side effect example folder')
    for btt in eg_exports.side_effect_paths():
      eg_exports.trigger_saved(btt, 'side effect example folder')
    i1 = lk.importCSV(filename='DATA$/side effect exports/a')
    i2 = lk.importCSV(filename='DATA$/side effect exports/b')
    self.assertEqual(i1.get_table_data().data[0][0].string, 'Isolated Joe')
    self.assertEqual(i2.get_table_data().data[0][0].string, 'Bob')

  def test_side_effects_from_different_boxes(self):
    lk = lynx.kite.LynxKite()

    @lk.workspace_with_side_effects(parameters=[text('snapshot_path')])
    def save_graph_to_snapshot(sec, graph):
      graph.sql('select * from vertices').saveToSnapshot(path=pp('$snapshot_path')).register(sec)

    @lk.workspace_with_side_effects(parameters=[text('snapshot_path')])
    def save_and_return_graph(sec, graph):
      graph.sql('select * from vertices').saveToSnapshot(path=pp('$snapshot_path')).register(sec)
      return dict(graph=graph)

    @lk.workspace_with_side_effects('Muliple graph snapshots')
    def eg_snapshots(sec):
      eg = lk.createExampleGraph()
      save_graph_to_snapshot(eg, snapshot_path='side effect snapshots/a').register(sec)
      first = save_and_return_graph(eg, snapshot_path='side effect snapshots/b')
      first.register(sec)
      save_graph_to_snapshot(first, snapshot_path='side effect snapshots/c').register(sec)

    eg_snapshots.save('side effect snapshots example folder')
    lk.remove_name('side effect snapshots', force=True)
    for btt in eg_snapshots.side_effect_paths():
      eg_snapshots.trigger_saved(btt, 'side effect snapshots example folder')
    entries = lk.list_dir('side effect snapshots')
    expected = [
        'side effect snapshots/a',
        'side effect snapshots/b',
        'side effect snapshots/c']
    self.assertEqual([e.name for e in entries], expected)

  def test_side_effects_unsaved_workspace(self):
    lk = lynx.kite.LynxKite()

    @lk.workspace_with_side_effects(parameters=[text('snapshot_path')])
    def save_graph_to_snapshot(sec, graph):
      graph.sql('select * from vertices').saveToSnapshot(path=pp('$snapshot_path')).register(sec)

    @lk.workspace_with_side_effects()
    def eg_snapshots(sec):
      eg = lk.createExampleGraph()
      save_graph_to_snapshot(eg, snapshot_path='unsaved/a').register(sec)
      save_graph_to_snapshot(eg, snapshot_path='unsaved/b').register(sec)

    lk.remove_name('unsaved', force=True)
    eg_snapshots.trigger_all_side_effects()
    entries = lk.list_dir('unsaved')
    expected = ['unsaved/a', 'unsaved/b']
    self.assertEqual([e.name for e in entries], expected)

  def test_trigger_single_side_effects_in_unsaved_workspace(self):
    lk = lynx.kite.LynxKite()

    @lk.workspace_with_side_effects(parameters=[text('snapshot_path')])
    def save_graph_to_snapshot(sec, graph):
      graph.sql('select * from vertices').saveToSnapshot(path=pp('$snapshot_path')).register(sec)

    @lk.workspace_with_side_effects()
    def snapshots(sec):
      eg = lk.createExampleGraph()
      save_graph_to_snapshot(eg, snapshot_path='single/a').register(sec)
      save_graph_to_snapshot(eg, snapshot_path='single/b').register(sec)

    lk.remove_name('single', force=True)
    for btt in snapshots.side_effect_paths():
      snapshots.trigger(btt)
    entries = lk.list_dir('single')
    expected = ['single/a', 'single/b']
    self.assertEqual([e.name for e in entries], expected)
