import unittest
from datetime import datetime, timedelta
import lynx.kite
import lynx.automation
import warnings
from test_dag_creation import create_complex_test_workspace


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
        schedule='* * * * *',
        start_date=datetime(2018, 5, 10),
        params={},
        lk_root='generated_airflow_dag_test',
        dfs_root='',
        input_recipes=[])
    trivial_eg_dag = wss.to_airflow_DAG('trivial_eg_dag')
    self.assertEqual(trivial_eg_dag.schedule_interval, '* * * * *')
    self.assertEqual(trivial_eg_dag.default_args['start_date'], datetime(2018, 5, 10))
    self.assertEqual(trivial_eg_dag.dag_id, 'trivial_eg_dag')
    self.assertEqual(trivial_eg_dag.owner, 'airflow')
    deps = deps_of_airflow_dag(trivial_eg_dag)
    expected = {
        'save_workspace': {
            'upstream': set(),
            'downstream': {'output_result'}},
        'output_result': {
            'upstream': {'save_workspace'},
            'downstream': set()}
    }
    self.assertEqual(deps, expected)

  def test_complex_dag(self):
    # We suppress deprecation warnings coming from Airflow
    warnings.simplefilter("ignore")
    lk = lynx.kite.LynxKite()
    wss = lynx.automation.WorkspaceSequence(
        ws=create_complex_test_workspace(),
        schedule='0 3 * * *',
        start_date=datetime(2018, 5, 11),
        params={},
        lk_root='generated_airflow_dag_test',
        dfs_root='',
        input_recipes=[])
    complex_dag = wss.to_airflow_DAG('complex_dag')
    self.assertEqual(complex_dag.schedule_interval, '0 3 * * *')
    self.assertEqual(complex_dag.default_args['start_date'], datetime(2018, 5, 11))
    self.assertEqual(complex_dag.dag_id, 'complex_dag')
    self.assertEqual(complex_dag.owner, 'airflow')
    deps = deps_of_airflow_dag(complex_dag)
    # Airflow stores the "direct" dependencies (aka minimal dag edges)
    # in `task.downstream_list` and `task.upstream_list`
    expected = {
        'input_i1': {
            'upstream': {'input_sensor_i1'},
            'downstream': {'snapshotter--saveToSnapshot_SB2'}},
        'input_sensor_i1': {
            'upstream': set(),
            'downstream': {'input_i1'}},
        'save_workspace': {
            'upstream': set(),
            'downstream': {
                'snapshotter--saveToSnapshot_SB1',
                'snapshotter--saveToSnapshot_SB2',
                'output_o3'}},
        'input_i3': {
            'upstream': {'input_sensor_i3'},
            'downstream': {'output_o2'}},
        'input_sensor_i3': {
            'upstream': set(),
            'downstream': {'input_i3'}},
        'input_i2': {
            'upstream': {'input_sensor_i2'},
            'downstream': {'snapshotter--saveToSnapshot_SB1'}},
        'input_sensor_i2': {
            'upstream': set(),
            'downstream': {'input_i2'}},
        'snapshotter--saveToSnapshot_SB2': {
            'upstream': {'input_i1', 'save_workspace'},
            'downstream': {'output_o1'}},
        'snapshotter--saveToSnapshot_SB1': {
            'upstream': {'input_i2', 'save_workspace'},
            'downstream': set()},
        'output_o3': {
            'upstream': {'save_workspace'},
            'downstream': set()},
        'output_o1': {
            'upstream': {'snapshotter--saveToSnapshot_SB2'},
            'downstream': {'output_o2'}},
        'output_o2': {
            'upstream': {'output_o1', 'input_i3'},
            'downstream': set()}}
    self.assertEqual(deps, expected)

  def test_input_sensors(self):
    # We suppress deprecation warnings coming from Airflow
    warnings.simplefilter("ignore")
    lk = lynx.kite.LynxKite()
    tss = lynx.kite.TableSnapshotSequence(lk, 'eq_table_seq', '0 3 * * *')
    lk.remove_name(tss.snapshot_name(datetime(2018, 5, 11, 3, 0)), force=True)
    lk.createExampleGraph().sql('select * from vertices').save_to_sequence(tss, datetime(2018, 5, 11, 3, 0))
    input_recipe = lynx.automation.TableSnapshotRecipe(tss)
    wss = lynx.automation.WorkspaceSequence(
        ws=create_complex_test_workspace(),
        schedule='0 3 * * *',
        start_date=datetime(2018, 5, 11),
        params={},
        lk_root='airflow_sensor_test',
        dfs_root='',
        input_recipes=[input_recipe] * 3)
    complex_dag = wss.to_airflow_DAG('complex_dag')
    sensor_tasks = [t for t in complex_dag.tasks if 'sensor' in t.task_id]
    for s in sensor_tasks:
      self.assertFalse(s.poke(dict(execution_date=datetime(2018, 5, 10, 3, 0))))
      self.assertTrue(s.poke(dict(execution_date=datetime(2018, 5, 11, 3, 0))))

  def test_airflow_dag_parameters(self):
    # We suppress deprecation warnings coming from Airflow
    warnings.simplefilter("ignore")
    lk = lynx.kite.LynxKite()
    wss = lynx.automation.WorkspaceSequence(
        ws=create_complex_test_workspace(),
        schedule='30 5 * * *',
        start_date=datetime(2018, 5, 11),
        params={},
        lk_root='airflow_dag_parameter_test',
        dfs_root='',
        input_recipes=[])
    param_dag_good = wss.to_airflow_DAG(
        'parametrized_dag',
        default_args=dict(depends_on_past=True))
    self.assertEqual(param_dag_good.default_args,
                     dict(owner='airflow',
                          start_date=datetime(2018, 5, 11, 0, 0),
                          depends_on_past=True))
    with self.assertRaises(Exception) as context:
      param_dag_bad = wss.to_airflow_DAG(
          'bad_parametrized_dag',
          default_args=dict(start_date=datetime(2018, 6, 11, 0, 0)))
    self.assertTrue('You cannot override start_date' in str(context.exception))
    param_dag_good2 = wss.to_airflow_DAG(
        'parametrized_dag2',
        max_active_runs=16,
        concurrency=48,
        default_args=dict(depends_on_past=True))
    self.assertEqual(param_dag_good2.max_active_runs, 16)
    self.assertEqual(param_dag_good2.concurrency, 48)
    self.assertEqual(param_dag_good2.default_args['depends_on_past'], True)
    self.assertEqual(param_dag_good2.schedule_interval, '30 5 * * *')
    with self.assertRaises(Exception) as context:
      param_dag_bad2 = wss.to_airflow_DAG(
          'bad_parametrized_dag2',
          schedule_interval='0 0 1 * *')
    self.assertTrue('You cannot override schedule_interval' in str(context.exception))
