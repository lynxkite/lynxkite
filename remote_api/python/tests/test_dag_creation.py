import unittest
import time
import lynx.kite


def boxPathToName(box_path):
  return box_path.base.parameters['name']


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

    expected_order = [4, 3, 2, 1]
    actual_order = [n for n in min_dag]
    self.assertEqual(actual_order, expected_order)

  def test_minimal_dag_on_two_paths(self):
    g = {
        1: {2, 3},
        2: {4},
        3: {4},
        4: set(),
    }
    min_dag = lynx.kite._minimal_dag(g)
    self.assertEqual(min_dag, g)

    order = [n for n in min_dag]
    self.assertEqual(order[0], 4)
    self.assertEqual(order[-1], 1)

  def create_complex_test_workspace(self):
    # Builds the workspace specified here:
    # https://docs.google.com/drawings/d/1YVGTdgrcjGuI-Z-jON7I3B6U3zOaFVUiT5lPnk2I7Kc/
    lk = lynx.kite.LynxKite()

    @lk.workspace()
    def forker(table):
      result = table.sql('select * from input', name='forker-sql')
      return dict(fork1=result, fork2=result)

    @lk.workspace_with_side_effects()
    def snapshotter(se_collector, t1_snap, t2_snap):
      t2_snap.saveToSnapshot(path='SB1', name='SB1').register(se_collector)
      tmp = forker(t1_snap)
      tmp['fork2'].saveToSnapshot(path='SB2', name='SB2').register(se_collector)
      return dict(out_snap=tmp['fork1'])

    @lk.workspace()
    def trivial():
      result = lk.createExampleGraph().sql('select * from vertices', name='trivial-sql')
      return dict(out_triv=result)

    @lk.workspace()
    def combiner(t1_comb, t2_comb):
      tmp = t1_comb.sql('select * from input', name='combiner-select')
      tmp2 = lk.sql('select * from one cross join two', tmp, t2_comb, name='combiner-join')
      return dict(out1_comb=tmp, out2_comb=tmp2)

    @lk.workspace_with_side_effects()
    def main_workspace(se_collector, i1, i2, i3):
      tmp = snapshotter(i1, i2)
      tmp.register(se_collector)
      internal = i3.sql('select * from input', name='main-select')
      last = combiner(tmp, internal)
      return dict(o1=last['out1_comb'], o2=last['out2_comb'], o3=trivial())

    return main_workspace

  def test_atomic_parents(self):
    # test parent of terminal boxes
    main_workspace = self.create_complex_test_workspace()
    parents = {}
    for box in main_workspace.output_boxes():
      ep = lynx.kite.BoxPath(box)
      parents[boxPathToName(ep)] = [boxPathToName(bp) for bp in ep.parents()]
    for ep in main_workspace.side_effect_paths():
      parents[boxPathToName(ep)] = [boxPathToName(bp) for bp in ep.parents()]

    expected = {
        'o1': ['out1_comb'],
        'o2': ['out2_comb'],
        'o3': ['out_triv'],
        'SB1': ['t2_snap'],
        'SB2': ['fork2'],
    }

    self.assertEqual(parents, expected)

  def test_non_trivial_atomic_parents(self):
    # test non-trivial parents of terminal boxes
    main_workspace = self.create_complex_test_workspace()
    actual = dict()
    for box in main_workspace.output_boxes():
      ep = lynx.kite.BoxPath(box)
      actual[boxPathToName(ep)] = boxPathToName(ep.non_trivial_parent_of_endpoint())
    for ep in main_workspace.side_effect_paths():
      actual[boxPathToName(ep)] = boxPathToName(ep.non_trivial_parent_of_endpoint())
    expected = {
        'o1': 'combiner-select',
        'o2': 'combiner-join',
        'o3': 'trivial-sql',
        'SB1': 'i2',
        'SB2': 'forker-sql',
    }
    self.assertEqual(actual, expected)

  def test_upstream_of_one_endpoint(self):
    # test full traversal of one endpoint
    main_workspace = self.create_complex_test_workspace()
    start = lynx.kite.BoxPath(
        [box for box in main_workspace.output_boxes()
         if box.parameters['name'] == 'o1'][0])
    up = start.parents()
    last = start
    while len(up) > 0:
      last = up[0]
      up = up[0].parents()
    expected = {'operation': 'fake input parent', 'params': {'name': 'i1'}, 'nested_in': None}
    self.assertEqual(last.to_dict(), expected)

  def test_endpoint_dependencies(self):
    main_workspace = self.create_complex_test_workspace()
    dependencies = {boxPathToName(ep): sorted([boxPathToName(dep) for dep in deps])
                    for ep, deps in main_workspace.automation_dependencies().items()}
    expected = {
        'i1': [],
        'i2': [],
        'i3': [],
        'o1': ['SB2', 'i1'],
        'o2': ['SB2', 'i1', 'i3', 'o1'],
        'SB1': ['i2'],
        'SB2': ['i1'],
        'o3': [],
    }
    self.assertEqual(dependencies, expected)

  def test_box_path_hash(self):
    lk = lynx.kite.LynxKite()
    from lynx.kite import BoxPath
    # eg1 and eg2 are different boxes, but with the same semantics.
    # The hash of bp1 and bp2 are the same, but bp1 and bp2 are different.
    eg1 = lk.createExampleGraph()
    eg2 = lk.createExampleGraph()
    bp1 = BoxPath(eg1)
    bp2 = BoxPath(eg2)
    bp3 = BoxPath(eg2)
    self.assertTrue(bp1.__hash__() == bp2.__hash__())
    self.assertTrue(bp1 != bp2)
    # bp1 and bp2 should be two different dict keys (they have the same hash but
    # they are not equal: we can have two different "Create Example Graph" boxes
    # in the same Workspace).
    d = {bp1: 'EG1', bp2: 'EG2'}
    self.assertEqual(len(d), 2)
    # bp2 and bp3 should be the same dict key, because they represent the same box.
    dd = {bp2: 'EG2'}
    self.assertEqual(dd[bp3], 'EG2')

  def test_dependency_computation_performance(self):
    lk = lynx.kite.LynxKite()

    @lk.workspace()
    def multi_source():
      eg = lk.createExampleGraph().sql('select * from vertices')
      return dict(t1=eg, t2=eg, t3=eg, t4=eg, t5=eg, t6=eg, t7=eg, t8=eg, t9=eg)

    @lk.workspace()
    def table_multiplier(table):
      return dict(t1=table, t2=table, t3=table,
                  t4=table, t5=table, t6=table,
                  t7=table, t8=table, t9=table)

    @lk.workspace()
    def reducer(t1, t2, t3, t4, t5, t6, t7, t8, t9):
      return dict(res=lk.sql('select 1 as value', t1, t2, t3, t4, t5, t6, t7, t8, t9))

    @lk.workspace()
    def big_workspace():
      source = multi_source()
      depth = 15
      layers = [{} for l in range(2 * depth)]
      for i in range(1, 10):
        layers[0][i] = table_multiplier(source[f't{i}'])
        layers[1][i] = reducer(*[layers[0][i][f't{j}'] for j in range(1, 10)])
        for d in range(1, depth):
          layers[2 * d][i] = table_multiplier(layers[2 * d - 1][i]['res'])
          layers[2 * d + 1][i] = reducer(*[layers[2 * d][i][f't{j}'] for j in range(1, 10)])
      result = reducer(*[layers[2 * depth - 1][j] for j in range(1, 10)])
      return dict(final=result)

    big_workspace.save('big folder')
    start_at = time.time()
    dep = big_workspace.automation_dependencies()
    elapsed = time.time() - start_at
    print(f'Dependency computation ran in {elapsed}s')
