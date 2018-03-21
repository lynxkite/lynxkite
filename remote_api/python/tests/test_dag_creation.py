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
    return lynx.kite.Workspace(name='test', terminal_boxes=[age.output(name='age'), age1, age2])

  def test_dependency_graph(self):
    ws = self.get_test_workspace()
    g = ws.dependency_graph()
    self.assertEqual(len(g), 3)
    dep_counts = Counter(len(deps) for deps in g.values())
    self.assertEqual(dep_counts[0], 1)
    self.assertEqual(dep_counts[1], 2)

  def test_minimal_dag_on_full_graph(self):
    size = 4
    g = {i: set(range(i + 1, size)) for i in range(size)}
    expected = {i: {i + 1} if i < size - 1 else set() for i in range(size)}
    min_dag = lynx.kite.minimal_dag(g)
    self.assertEqual(min_dag, expected)

  def test_minimal_dag_on_two_paths(self):
    g = {
        1: {2, 3},
        2: {4},
        3: {4},
        4: set(),
    }
    min_dag = lynx.kite.minimal_dag(g)
    self.assertEqual(min_dag, g)
