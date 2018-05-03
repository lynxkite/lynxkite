import unittest
import lynx.kite
import json
from collections import Counter
import time


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

  def test_minimal_dag_on_two_paths(self):
    g = {
        1: {2, 3},
        2: {4},
        3: {4},
        4: set(),
    }
    min_dag = lynx.kite._minimal_dag(g)
    self.assertEqual(min_dag, g)

  def create_complex_test_workspace(self):
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

    return main_workspace

  def test_atomic_parents(self):
    # test parent of terminal boxes
    main_workspace = self.create_complex_test_workspace()
    parents = {}
    for box in main_workspace.output_boxes():
      ep = lynx.kite.BoxPath(box)
      parents[str(ep.to_dict())] = [bp.to_dict() for bp in ep.parents()]
    for ep in main_workspace.side_effect_paths():
      parents[str(ep.to_dict())] = [bp.to_dict() for bp in ep.parents()]

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

  def test_non_trivial_atomic_parents(self):
    # test non-trivial parents of terminal boxes
    main_workspace = self.create_complex_test_workspace()
    expected_ntp_of_outputs = {
        'o1': {'operation': 'sql1',
               'params': {'sql': 'select * from input'},
               'nested_in': 'combiner', },
        'o2': {'operation': 'sql2',
               'params': {'sql': 'select * from one cross join two'},
               'nested_in': 'combiner', },
        'o3': {'operation': 'sql1',
               'params': {'sql': 'select * from vertices'},
               'nested_in': 'trivial', },
    }
    for box in main_workspace.output_boxes():
      ep = lynx.kite.BoxPath(box)
      expected = expected_ntp_of_outputs[ep.to_dict()['params']['name']]
      self.assertEqual(ep.non_trivial_parent_of_endpoint().to_dict(), expected)

    expected_ntp_of_side_effects = {
        'SB1': {'operation': 'input',
                'params': {'name': 'i2'},
                'nested_in': None, },
        'SB2': {'operation': 'sql1',
                'params': {'sql': 'select * from input'},
                'nested_in': 'forker', },
    }
    for ep in main_workspace.side_effect_paths():
      expected = expected_ntp_of_side_effects[ep.to_dict()['params']['path']]
      self.assertEqual(ep.non_trivial_parent_of_endpoint().to_dict(), expected)

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
    raw_dep = main_workspace.automation_dependencies()
    dependencies = {str(ep.to_dict()): set() for ep in raw_dep}
    for ep, deps in raw_dep.items():
      for dep in deps:
        dependencies[str(ep.to_dict())].add(str(dep.to_dict()))
    expected_dependencies = {
        "{'operation': 'output', 'params': {'name': 'o1'}, 'nested_in': None}":
        {
            "{'operation': 'input', 'params': {'name': 'i1'}, 'nested_in': None}",
            "{'operation': 'saveToSnapshot', 'params': {'path': 'SB2'}, 'nested_in': 'snapshotter'}"
        },
        "{'operation': 'output', 'params': {'name': 'o2'}, 'nested_in': None}":
        {
            "{'operation': 'input', 'params': {'name': 'i1'}, 'nested_in': None}",
            "{'operation': 'output', 'params': {'name': 'o1'}, 'nested_in': None}",
            "{'operation': 'input', 'params': {'name': 'i3'}, 'nested_in': None}",
            "{'operation': 'saveToSnapshot', 'params': {'path': 'SB2'}, 'nested_in': 'snapshotter'}"
        },
        "{'operation': 'saveToSnapshot', 'params': {'path': 'SB1'}, 'nested_in': 'snapshotter'}":
        {
            "{'operation': 'input', 'params': {'name': 'i2'}, 'nested_in': None}"
        },
        "{'operation': 'saveToSnapshot', 'params': {'path': 'SB2'}, 'nested_in': 'snapshotter'}":
        {
            "{'operation': 'input', 'params': {'name': 'i1'}, 'nested_in': None}"
        },
    }
    self.assertEqual(expected_dependencies, dependencies)

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
