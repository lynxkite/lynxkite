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
    def snapshotter(se_collector, t1, t2):
      t2.saveToSnapshot(path='SB1').register(se_collector)
      tmp = forker(t1)
      tmp['fork2'].saveToSnapshot(path='SB2').register(se_collector)
      return dict(out=tmp['fork1'])

    @lk.workspace()
    def trivial():
      result = lk.createExampleGraph().sql('select * from vertices')
      return dict(out=result)

    @lk.workspace()
    def combiner(t1, t2):
      tmp = t1.sql('select * from input')
      tmp2 = lk.sql('select * from one cross join two', tmp, t2)
      return dict(out1=tmp, out2=tmp2)

    @lk.workspace_with_side_effects()
    def main_workspace(se_collector, i1, i2, i3):
      tmp = snapshotter(i1, i2)
      tmp.register(se_collector)
      internal = i3.sql('select * from input')
      last = combiner(tmp, internal)
      return dict(o1=last['out1'], o2=last['out2'], o3=trivial())

    def box_path_desc(box_path):
      op = box_path.box_stack[-1].operation
      op_param = box_path.box_stack[-1].parameters
      info = op + '(' + str(op_param) + ')'
      if len(box_path.box_stack) > 1:
        parent = box_path.box_stack[-2].operation.name()
        info += ' inside ' + parent
      return info

    def endpoit_info(ep):
      print('  Endpoint: ' + box_path_desc(ep))
      print('    Parents of endpoint: ' + str([box_path_desc(bp) for bp in ep.parents()]))

    print('Outputs')
    for box in main_workspace.output_boxes():
      ep = lynx.kite.BoxPath([box])
      endpoit_info(ep)

    print('Side effects')
    for ep in main_workspace.side_effects().boxes_to_trigger:
      endpoit_info(ep)
