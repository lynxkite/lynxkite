import unittest
import lynx.kite
import json
from collections import Counter


class TestDagCreation(unittest.TestCase):

  def get_test_workspace(self):
    lk = lynx.kite.LynxKite()

    @lk.workspace()
    def select_age(p):
      return {'ages': p.sql('select age from vertices')}

    eg = lk.createExampleGraph()
    age = select_age(eg)
    age1 = age.sql('select age + 1 as age1 from t').output(name='age1')
    age2 = age.sql('select age + 2 as age2 from t').output(name='age2')
    return lynx.kite.Workspace(name='test', output_boxes=[age.output(name='age'), age1, age2])

  def test_dependency_graph(self):
    ws = self.get_test_workspace()
    g = ws.dependency_graph()
    self.assertEqual(len(g), 3)
    boxes_with_zero_dependency = [b for b, deps in g.items() if len(deps) == 0]
    self.assertEqual(len(boxes_with_zero_dependency), 1)
    age = boxes_with_zero_dependency[0]
    other_boxes = (b for b in g if b != age)
    for box in other_boxes:
      self.assertEqual(g[box], {age})

  def test_minimal_dag_on_full_graph(self):
    g = {
        1: {2, 3, 4},
        2: {3, 4},
        3: {4},
        4: set(),
    }
    expected = {
        1: {2},
        2: {3},
        3: {4},
        4: set(),
    }
    min_dag = lynx.kite._minimal_dag(g)
    self.assertEqual(min_dag, expected)

  def test_minimal_dag_on_two_paths(self):
    g = {
        1: {2, 3},
        2: {4},
        3: {4},
        4: set(),
    }
    min_dag = lynx.kite._minimal_dag(g)
    self.assertEqual(min_dag, g)

  def test_atomic_parents(self):
    # Builds the workspace specified here:
    # https://docs.google.com/drawings/d/1YVGTdgrcjGuI-Z-jON7I3B6U3zOaFVUiT5lPnk2I7Kc/
    lk = lynx.kite.LynxKite()

    @lk.workspace()
    def forker(table):
      result = table.sql('select * from input')
      return dict(fork1=result, fork2=result)

    @lk.workspace_with_side_effects()
    def snapshotter(se_collector, t1_snap, t2_snap):
      t2_snap.saveToSnapshot(path='SB1').register(se_collector)
      tmp = forker(t1_snap)
      tmp['fork2'].saveToSnapshot(path='SB2').register(se_collector)
      return dict(out_snap=tmp['fork1'])

    @lk.workspace()
    def trivial():
      result = lk.createExampleGraph().sql('select * from vertices')
      return dict(out_triv=result)

    @lk.workspace()
    def combiner(t1_comb, t2_comb):
      tmp = t1_comb.sql('select * from input')
      tmp2 = lk.sql('select * from one cross join two', tmp, t2_comb)
      return dict(out1_comb=tmp, out2_comb=tmp2)

    @lk.workspace_with_side_effects()
    def main_workspace(se_collector, i1, i2, i3):
      tmp = snapshotter(i1, i2)
      tmp.register(se_collector)
      internal = i3.sql('select * from input')
      last = combiner(tmp, internal)
      return dict(o1=last['out1_comb'], o2=last['out2_comb'], o3=trivial())

    def box_path_desc(box_path):
      parent = None
      op = box_path.box_stack[-1].operation
      op_param = box_path.box_stack[-1].parameters
      if len(box_path.box_stack) > 1:
        parent = box_path.box_stack[-2].operation.name()
      return dict(operation=op, params=op_param, nested_in=parent)

    parents = {}
    for box in main_workspace.output_boxes():
      ep = lynx.kite.BoxPath([box])
      parents[str(box_path_desc(ep))] = [box_path_desc(bp) for bp in ep.parents()]
    for ep in main_workspace.side_effects().boxes_to_trigger:
      parents[str(box_path_desc(ep))] = [box_path_desc(bp) for bp in ep.parents()]

    expected = {
        "{'operation': 'output', 'params': {'name': 'o1'}, 'nested_in': None}":
        [{'operation': 'output',
          'params': {'name': 'out1_comb'},
          'nested_in': 'combiner'}],
        "{'operation': 'output', 'params': {'name': 'o2'}, 'nested_in': None}":
        [{'operation': 'output',
          'params': {'name': 'out2_comb'},
          'nested_in': 'combiner'}],
        "{'operation': 'output', 'params': {'name': 'o3'}, 'nested_in': None}":
        [{'operation': 'output',
          'params': {'name': 'out_triv'},
          'nested_in': 'trivial'}],
        "{'operation': 'saveToSnapshot', 'params': {'path': 'SB1'}, 'nested_in': 'snapshotter'}":
        [{'operation': 'input',
          'params': {'name': 't2_snap'},
          'nested_in': 'snapshotter'}],
        "{'operation': 'saveToSnapshot', 'params': {'path': 'SB2'}, 'nested_in': 'snapshotter'}":
        [{'operation': 'output',
          'params': {'name': 'fork2'},
          'nested_in': 'forker'}]}

    self.assertEqual(parents, expected)
