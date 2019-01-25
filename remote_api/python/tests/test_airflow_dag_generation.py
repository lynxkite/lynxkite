import unittest
from datetime import datetime, timedelta
import lynx.kite
import lynx.automation
from lynx.automation import Schedule, utc_dt
import warnings
import random
import re
from test_dag_creation import create_complex_test_workspace, create_test_wss
from test_dag_creation import create_test_workspace_with_ugly_ids


def deps_of_airflow_dag(airflow_dag):
  deps = {}
  for task in airflow_dag.tasks:
    deps[task.task_id] = dict(
        upstream={t.task_id for t in task.upstream_list},
        downstream={t.task_id for t in task.downstream_list})
  return deps


class TestAirflowDagGeneration(unittest.TestCase):

  def test_trivial_dag(self):
    # We suppress deprecation warnings coming from Airflow
    warnings.simplefilter("ignore")
    lk = lynx.kite.LynxKite()

    @lk.workspace()
    def trivial():
      return dict(result=lk.createExampleGraph().sql('select name, age from vertices'))

    wss = lynx.automation.WorkspaceSequence(
        ws=trivial,
        schedule=Schedule(utc_dt(2018, 5, 10), '* * * * *'),
        lk_root='generated_airflow_dag_test',
        input_recipes=[])
    trivial_eg_dag = wss.to_airflow_DAG('trivial_eg_dag')
    self.assertEqual(trivial_eg_dag.schedule_interval, '* * * * *')
    self.assertEqual(trivial_eg_dag.default_args['start_date'], utc_dt(2018, 5, 10))
    self.assertEqual(trivial_eg_dag.dag_id, 'trivial_eg_dag')
    self.assertEqual(trivial_eg_dag.owner, 'airflow')
    deps = deps_of_airflow_dag(trivial_eg_dag)
    expected = {
        'save_workspace': {
            'upstream': set(),
            'downstream': {'output_result'}},
        'output_result': {
            'upstream': {'save_workspace'},
            'downstream': {'run_cleaner'}},
        'run_cleaner': {
            'upstream': {'output_result'},
            'downstream': set()}
    }
    self.assertEqual(deps, expected)

  def test_complex_dag(self):
    # We suppress deprecation warnings coming from Airflow
    warnings.simplefilter("ignore")
    lk = lynx.kite.LynxKite()
    wss = lynx.automation.WorkspaceSequence(
        ws=create_complex_test_workspace(),
        schedule=Schedule(utc_dt(2018, 5, 11), '0 3 * * *'),
        lk_root='generated_airflow_dag_test',
        input_recipes=[])
    complex_dag = wss.to_airflow_DAG('complex_dag')
    self.assertEqual(complex_dag.schedule_interval, '0 3 * * *')
    self.assertEqual(complex_dag.default_args['start_date'], utc_dt(2018, 5, 11))
    self.assertEqual(complex_dag.dag_id, 'complex_dag')
    self.assertEqual(complex_dag.owner, 'airflow')
    deps = deps_of_airflow_dag(complex_dag)
    # Airflow stores the "direct" dependencies (aka minimal dag edges)
    # in `task.downstream_list` and `task.upstream_list`
    expected = {
        'input_i1': {
            'upstream': {'input_sensor_i1'},
            'downstream': {'trigger_snapshotter_0_saveToSnapshot_1'}},
        'input_sensor_i1': {
            'upstream': set(),
            'downstream': {'input_i1'}},
        'save_workspace': {
            'upstream': set(),
            'downstream': {
                'trigger_snapshotter_0_saveToSnapshot_0',
                'trigger_snapshotter_0_saveToSnapshot_1',
                'output_o3'}},
        'input_i3': {
            'upstream': {'input_sensor_i3'},
            'downstream': {'output_o2'}},
        'input_sensor_i3': {
            'upstream': set(),
            'downstream': {'input_i3'}},
        'input_i2': {
            'upstream': {'input_sensor_i2'},
            'downstream': {'trigger_snapshotter_0_saveToSnapshot_0'}},
        'input_sensor_i2': {
            'upstream': set(),
            'downstream': {'input_i2'}},
        'trigger_snapshotter_0_saveToSnapshot_1': {
            'upstream': {'input_i1', 'save_workspace'},
            'downstream': {'output_o1'}},
        'trigger_snapshotter_0_saveToSnapshot_0': {
            'upstream': {'input_i2', 'save_workspace'},
            'downstream': {'run_cleaner'}},
        'output_o3': {
            'upstream': {'save_workspace'},
            'downstream': {'run_cleaner'}},
        'output_o1': {
            'upstream': {'trigger_snapshotter_0_saveToSnapshot_1'},
            'downstream': {'output_o2'}},
        'output_o2': {
            'upstream': {'output_o1', 'input_i3'},
            'downstream': {'run_cleaner'}},
        'run_cleaner': {
            'upstream': {
                'output_o3',
                'output_o2',
                'trigger_snapshotter_0_saveToSnapshot_0'},
            'downstream': set()}}
    self.assertEqual(deps, expected)

  def test_input_sensors(self):
    # We suppress deprecation warnings coming from Airflow
    warnings.simplefilter("ignore")
    lk = lynx.kite.LynxKite()
    tss = lynx.automation.TableSnapshotSequence(
        lk,
        'eq_table_seq',
        Schedule(utc_dt(2018, 5, 11), '0 3 * * *'))
    tss.remove_date(utc_dt(2018, 5, 11, 3, 0))
    lk.createExampleGraph().sql('select * from vertices').save_to_sequence(
        tss,
        utc_dt(2018, 5, 11, 3, 0))
    input_recipe = lynx.automation.TableSnapshotRecipe(tss)
    wss = lynx.automation.WorkspaceSequence(
        ws=create_complex_test_workspace(),
        schedule=Schedule(utc_dt(2018, 5, 11), '0 3 * * *'),
        lk_root='airflow_sensor_test',
        input_recipes=[input_recipe] * 3)
    complex_dag = wss.to_airflow_DAG('complex_dag')
    sensor_tasks = [t for t in complex_dag.tasks if 'sensor' in t.task_id]
    for s in sensor_tasks:
      self.assertTrue(s.poke(dict(execution_date=utc_dt(2018, 5, 11, 3, 0))))
      self.assertFalse(s.poke(dict(execution_date=utc_dt(2018, 5, 12, 3, 0))))

  def test_airflow_dag_parameters(self):
    # We suppress deprecation warnings coming from Airflow
    warnings.simplefilter("ignore")
    lk = lynx.kite.LynxKite()
    wss = lynx.automation.WorkspaceSequence(
        ws=create_complex_test_workspace(),
        schedule=Schedule(utc_dt(2018, 5, 11), '30 5 * * *'),
        lk_root='airflow_dag_parameter_test',
        input_recipes=[])
    param_dag_good = wss.to_airflow_DAG(
        'parametrized_dag',
        task_default_args=dict(depends_on_past=True))
    self.assertEqual(param_dag_good.default_args,
                     dict(owner='airflow',
                          start_date=utc_dt(2018, 5, 11, 0, 0),
                          depends_on_past=True))
    with self.assertRaises(Exception) as context:
      param_dag_bad = wss.to_airflow_DAG(
          'bad_parametrized_dag',
          task_default_args=dict(start_date=datetime(2018, 6, 11, 0, 0)))
    self.assertTrue('You cannot override start_date' in str(context.exception))
    param_dag_good2 = wss.to_airflow_DAG(
        'parametrized_dag2',
        dag_args=dict(max_active_runs=16, concurrency=48),
        task_default_args=dict(depends_on_past=True))
    self.assertEqual(param_dag_good2.max_active_runs, 16)
    self.assertEqual(param_dag_good2.concurrency, 48)
    self.assertEqual(param_dag_good2.default_args['depends_on_past'], True)
    self.assertEqual(param_dag_good2.schedule_interval, '30 5 * * *')
    with self.assertRaises(Exception) as context:
      param_dag_bad2 = wss.to_airflow_DAG(
          'bad_parametrized_dag2',
          dag_args=dict(schedule_interval='0 0 1 * *'))
    self.assertTrue('You cannot override schedule_interval' in str(context.exception))

  def test_generated_airflow_task_ids(self):
    # We suppress deprecation warnings coming from Airflow
    warnings.simplefilter("ignore")
    lk = lynx.kite.LynxKite()

    # Original test wss
    wss = create_test_wss(create_complex_test_workspace())
    self.assertEqual(set(wss.to_airflow_DAG('task_id_dag').task_ids),
                     {'input_i1', 'input_sensor_i1', 'save_workspace', 'input_i3',
                      'input_sensor_i3', 'input_i2', 'input_sensor_i2',
                      'trigger_snapshotter_0_saveToSnapshot_1', 'output_o3',
                      'trigger_snapshotter_0_saveToSnapshot_0', 'output_o1',
                      'output_o2', 'run_cleaner'})

    # RAIN wss
    # TODO :rewrite this after we implemented nice task ids
    wss2 = create_test_wss(create_test_workspace_with_ugly_ids())
    task_ids = wss2.to_airflow_DAG('task_id_dag').task_ids
    self.assertEqual(set(task_ids),
                     {'input_site', 'input_oss_user_average30',
                      'output_active_cells', 'output_available_cells',
                      'input_sensor_site', 'save_workspace',
                      'input_sensor_oss_user_average30',
                      'trigger_where_to_sell_0_exportToCSV_0',
                      'trigger_where_to_sell_0_exportToCSV_1',
                      'input_oss_combined30', 'input_sensor_oss_combined30',
                      'run_cleaner'})
    for task_id in task_ids:
      self.assertTrue(len(task_id) <= 250)
      self.assertIsNone(re.search(r'[^0-9a-zA-Z\-\.\_]', task_id))

    # Test wss with long id
    # random string from 'X', 'Y' and 'Z' with length 300
    table_param = ''.join(random.choices('XYZ', k=300))
    table_param2 = table_param[:299]

    @lk.workspace_with_side_effects(parameters=[lynx.kite.text('date')])
    def export_eg(se_collector):
      exp = lk.createExampleGraph().exportToParquet(
          table=lynx.kite.pp(f'${{{table_param}}}'), _id=table_param)
      exp.register(se_collector)
      exp2 = lk.createExampleGraph().exportToParquet(
          table=lynx.kite.pp(f'${{{table_param2}}}'), _id=table_param2)
      exp2.register(se_collector)
      return dict(g=lk.createExampleGraph())

    wss3 = create_test_wss(export_eg)
    task_ids = wss3.to_airflow_DAG('task_id_dag').task_ids
    self.assertEqual(len(task_ids), 5)  # output, save, 2 exports, cleaner
    export_task_ids = [tid for tid in task_ids if (not 'save' in tid) and (not 'output' in tid)]
    self.assertEqual(export_task_ids[0][:218], export_task_ids[1][:218])
    self.assertNotEqual(export_task_ids[0][218:], export_task_ids[1][218:])

    for task_id in task_ids:
      self.assertTrue(len(task_id) <= 250)
      self.assertIsNone(re.search(r'[^0-9a-zA-Z\-\.\_]', task_id))

  def test_copy_for_airflow_clear_task(self):
    ''' Airflow may call copy/deepcopy on lk or on a box. '''
    lk = lynx.kite.LynxKite()
    box = lk.createExampleGraph()
    import copy
    lk2 = copy.copy(lk)
    self.assertEqual(len(lk._box_catalog.box_names()), len(lk2._box_catalog.box_names()))
    lk3 = copy.deepcopy(lk)
    self.assertEqual(len(lk._box_catalog.box_names()), len(lk3._box_catalog.box_names()))
    box2 = copy.copy(box)
    self.assertEqual(box.outputs, box2.outputs)
    box3 = copy.deepcopy(box)
    self.assertEqual(box.outputs, box3.outputs)
