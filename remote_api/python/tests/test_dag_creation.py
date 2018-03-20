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
    ages = select_age(eg)
    ages1 = ages.sql('select age + 1 as age1 from t')
    ages2 = ages.sql('select age + 2 as age2 from t')
    return lynx.kite.Workspace(name='test', terminal_boxes=[ages, ages1, ages2])

  def test_dependency_graph(self):
    ws = self.get_test_workspace()
    g = ws.dependency_graph()
    self.assertEqual(len(g), 3)
    dep_counts = Counter(len(deps) for deps in g.values())
    self.assertEqual(dep_counts[0], 1)
    self.assertEqual(dep_counts[1], 2)
