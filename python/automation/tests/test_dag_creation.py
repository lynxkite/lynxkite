import unittest
import time
from datetime import datetime, timedelta
import lynx.kite
import lynx.automation
from lynx.automation import Schedule, utc_dt


def _box_path_to_str(box_path):
  box = box_path.base
  op = box.operation
  p = box.parameters
  if op in ['sql1', 'sql2']:
    return f'sql {p["sql"]}'
  elif op == 'saveToSnapshot':
    return f'snapshot {p["path"]}'
  elif op in ['input', 'output']:
    return f'{op} {p["name"]}'
  else:
    raise NotImplementedError(f'No str conversion for {op}')


def _task_to_str(task):
  if isinstance(task, lynx.automation.BoxTask):
    return _box_path_to_str(task.box_path)
  elif isinstance(task, lynx.automation.SaveWorkspace):
    return 'save workspace'
  elif isinstance(task, lynx.automation.RunCleaner):
    return 'run cleaner'
  else:
    raise NotImplementedError(f'No str conversion for task {type(task)}')


def create_complex_test_workspace():
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
    tmp2 = lk.sql('select x.* from x cross join y', x=tmp, y=t2_comb)
    return dict(out1_comb=tmp, out2_comb=tmp2)

  @lk.workspace_with_side_effects()
  def main_workspace(se_collector, i1, i2, i3):
    tmp = snapshotter(i1, i2)
    tmp.register(se_collector)
    internal = i3.sql('select * from input')
    last = combiner(tmp, internal)
    return dict(o1=last['out1_comb'], o2=last['out2_comb'], o3=trivial())

  return main_workspace


def create_test_wss(ws):
  schedule = Schedule(utc_dt(2018, 7, 1), '0 0 * * *')
  return lynx.automation.WorkspaceSequence(
      ws=ws,
      schedule=schedule,
      lk_root='airflow_test_wss',
      input_recipes=[])


def create_test_workspace_with_ugly_ids():
  ''' Mock workspace from RAIN project. '''

  def _export_path() -> str:
    base_dir = 'rain/CELL_AVAILABILITY_LISTS/'
    active_suffix = 'CELL_COUNT_SUFFIX'
    model_v = '1.0'
    scala = 'date.replace("-", "").split(" ")(0)'
    return {
        'available_cells': f'ROOT$${base_dir}/${{{scala}}}v{model_v}',
        'active_cells': f'ROOT$${base_dir}/${{{scala}}}v{model_v}{active_suffix}',
    }

  lk = lynx.kite.LynxKite()

  @lk.workspace()
  def site_availability(oss_combined30, oss_user_average30, site):
    daily_usage = lk.sql('''
      select * from one left join two on one.site_id = two.site_id
    ''', oss_combined30, oss_user_average30)

    usage_ratios = daily_usage.sql('''
      select * from input where rain_rtth_down_day > 0
    ''')

    cellsite_criteria = lk.sql('''
      select * from usage left join site on usage.site_id = site.id cross join ratios
    ''', usage=daily_usage, site=site, ratios=usage_ratios)

    available_cells = cellsite_criteria.sql('''select * from input''', persist='yes')

    active_cells = oss_combined30.sql(
        'select site_id, count(distinct cell_id) as cell_count from input group by site_id',
        persist='yes')

    return {
        'available_cells': available_cells,
        'active_cells': active_cells,
    }

  @lk.workspace_with_side_effects(parameters=[lynx.kite.text('date')])
  def where_to_sell(sec, oss_combined, oss_user_average, site):
    sa = site_availability(oss_combined, oss_user_average, site)
    available_cells = sa['available_cells']
    active_cells = sa['active_cells']
    paths = _export_path()
    available_cells.exportToCSV(path=lynx.kite.pp(paths['available_cells'])).register(sec)
    active_cells.exportToCSV(path=lynx.kite.pp(paths['active_cells'])).register(sec)
    return {
        'available_cells': available_cells,
        'active_cells': active_cells,
    }

  @lk.workspace_with_side_effects(parameters=[lynx.kite.text('date')])
  def rain(sec, oss_combined30, oss_user_average30, site):
    wts = where_to_sell(oss_combined30, oss_user_average30, site, date=lynx.kite.pp('${date}'))
    wts.register(sec)
    return {
        'available_cells': wts['available_cells'],
        'active_cells': wts['active_cells'],
    }

  return rain


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
    min_dag = lynx.automation._minimal_dag(g)
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
    min_dag = lynx.automation._minimal_dag(g)
    self.assertEqual(min_dag, g)

    order = [n for n in min_dag]
    self.assertEqual(order[0], 4)
    self.assertEqual(order[-1], 1)

  test_date = utc_dt(2018, 1, 2)
  utc_test_date = utc_dt(2018, 1, 2)

  def complex_workspace_sequence(self):
    ws = create_complex_test_workspace()
    lk = ws.lk
    lk.remove_name('eq_table_seq', force=True)
    tss = lynx.automation.TableSnapshotSequence(
        lk,
        'eq_table_seq',
        Schedule(self.test_date, '0 0 * * *'))
    lk.createExampleGraph().sql('select * from vertices').save_to_sequence(
        tss,
        self.utc_test_date)
    input_recipe = lynx.automation.TableSnapshotRecipe(tss)
    day_before = self.test_date - timedelta(days=1)
    schedule = Schedule(day_before, '0 0 * * *')
    return lynx.automation.WorkspaceSequence(ws, schedule=schedule,
                                             lk_root='ws_seqence_to_dag', input_recipes=[input_recipe] * 3)

  def test_atomic_parents(self):
    # test parent of terminal boxes
    main_workspace = create_complex_test_workspace()
    parents = {}
    for box in main_workspace.output_boxes:
      ep = lynx.kite.BoxPath(box)
      parents[_box_path_to_str(ep)] = [_box_path_to_str(bp) for bp in ep.parents()]
    for ep in main_workspace.side_effect_paths():
      parents[_box_path_to_str(ep)] = [_box_path_to_str(bp) for bp in ep.parents()]

    expected = {
        'output o1': ['output out1_comb'],
        'output o2': ['output out2_comb'],
        'output o3': ['output out_triv'],
        'snapshot SB1': ['input t2_snap'],
        'snapshot SB2': ['output fork2'],
    }

    self.assertEqual(parents, expected)

  def test_dependency_representatives(self):
    main_workspace = create_complex_test_workspace()
    actual = dict()
    for box in main_workspace.output_boxes:
      ep = lynx.kite.BoxPath(box)
      actual[_box_path_to_str(ep)] = _box_path_to_str(ep.dependency_representative())
    for ep in main_workspace.side_effect_paths():
      actual[_box_path_to_str(ep)] = _box_path_to_str(ep.dependency_representative())
    expected = {
        'output o1': 'sql select * from input',
        'output o2': 'sql select x.* from x cross join y',
        'output o3': 'sql select * from vertices',
        'snapshot SB1': 'input i2',
        'snapshot SB2': 'sql select * from input',
    }
    self.assertEqual(actual, expected)

  def test_upstream_of_one_endpoint(self):
    # test full traversal of one endpoint
    main_workspace = create_complex_test_workspace()
    start = lynx.kite.BoxPath(
        [box for box in main_workspace.output_boxes
         if box.parameters['name'] == 'o1'][0])
    up = start.parents()
    last = start
    while len(up) > 0:
      last = up[0]
      up = up[0].parents()
    expected = {'operation': 'input', 'params': {'name': 'i1'}, 'nested_in': None}
    self.assertEqual(last.to_dict(), expected)

  def test_wss_box_dependencies(self):
    self.maxDiff = None
    wss = self.complex_workspace_sequence()
    dag = {task: set() for task in wss._automation_tasks()
           if not isinstance(task, lynx.automation.SaveWorkspace)}
    wss._add_box_based_dependencies(dag)
    dependencies = {_task_to_str(ep): sorted([_task_to_str(dep) for dep in deps])
                    for ep, deps in dag.items()}
    expected = {
        'input i1': [],
        'input i2': [],
        'input i3': [],
        'output o1': ['input i1', 'snapshot SB2'],
        'output o2': ['input i1', 'input i3', 'output o1', 'snapshot SB2'],
        'snapshot SB1': ['input i2'],
        'snapshot SB2': ['input i1'],
        'output o3': [],
        'run cleaner': [],
    }
    self.assertEqual(expected, dependencies)

  def test_wss_to_dag(self):
    dag = self.complex_workspace_sequence().to_dag()
    dependencies = {_task_to_str(ep): sorted([_task_to_str(dep) for dep in deps])
                    for ep, deps in dag.items()}
    expected = {
        'input i1': [],
        'input i2': [],
        'input i3': [],
        'snapshot SB2': ['input i1', 'save workspace'],
        'output o1': ['snapshot SB2'],
        'snapshot SB1': ['input i2', 'save workspace'],
        'output o2': ['input i3', 'output o1'],
        'save workspace': [],
        'output o3': ['save workspace'],
        'run cleaner': ['output o2', 'output o3', 'snapshot SB1'],
    }
    self.assertEqual(expected, dependencies)

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

    schedule = Schedule(utc_dt(2018, 1, 1), '0 0 * * *')
    wss = lynx.automation.WorkspaceSequence(big_workspace, schedule=schedule,
                                            lk_root='big folder', input_recipes=[])
    start_at = time.time()
    wss.to_dag()
    elapsed = time.time() - start_at
    print(f'Dependency computation ran in {elapsed}s')

  def test_wss_to_dag_order(self):
    dag = self.complex_workspace_sequence().to_dag()
    order = [t for t in dag]
    for i, task in enumerate(order):
      for previous in order[:i]:
        self.assertFalse(task in dag[previous])

  def test_dag_task_ids(self):
    dag = self.complex_workspace_sequence().to_dag()
    task_ids = set([t.id() for t in dag])
    expected = set([
        'input_i1', 'input_i3', 'save_workspace', 'input_i2',
        'trigger_snapshotter_0/saveToSnapshot_1',
        'trigger_snapshotter_0/saveToSnapshot_0',
        'output_o3', 'output_o1', 'output_o2', 'run_cleaner'])
    self.assertEqual(task_ids, expected)

  def test_wss_dag_is_runnable(self):
    wss = self.complex_workspace_sequence()
    dag = wss.to_dag()
    lk = wss.lk
    lk.remove_name('SB1', force=True)
    lk.remove_name('SB2', force=True)
    for t in dag:
      t.run(self.utc_test_date)
    for o in wss.output_sequences.values():
      self.assertTrue(lynx.automation.TableSnapshotRecipe(o).is_ready(self.utc_test_date))
    # is everything idempotent?
    for t in dag:
      t.run(self.utc_test_date)

  def test_save_workspace_task_is_rerunnable(self):
    wss = self.complex_workspace_sequence()
    dag = wss.to_dag()
    lk = wss.lk
    wssi = wss.wssi_for_date(self.utc_test_date)
    lk.remove_name(wssi.base_folder_name(), force=True)
    self.assertFalse(wssi.is_saved())
    save_task = [t for t in dag if isinstance(t, lynx.automation.SaveWorkspace)][0]
    save_task.run(self.utc_test_date)
    self.assertTrue(wssi.is_saved())
    # second save doesn't fail, just rewrites the wssi
    save_task.run(self.utc_test_date)
    self.assertTrue(wssi.is_saved())

  def test_dag_rerun_with_external_box(self):

    @lynx.kite.external
    def f(table):
      return table.lk().sql('select 4 as x from input limit 1')

    lk = lynx.kite.LynxKite()

    @lk.workspace_with_side_effects()
    def pipeline(sec):
      eg = lk.createExampleGraph().sql('select * from vertices')
      result = f(eg)
      result.register(sec)
      return result

    wss = lynx.automation.WorkspaceSequence(
        ws=pipeline,
        schedule=Schedule(utc_dt(2019, 8, 1), '0 0 * * *'),
        lk_root='rerun_test',
        input_recipes=[]
    )
    wss.run_dag_tasks(utc_dt(2019, 8, 1))
    wss.run_dag_tasks(utc_dt(2019, 8, 1))
