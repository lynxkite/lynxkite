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
