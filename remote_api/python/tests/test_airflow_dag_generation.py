import unittest
from datetime import datetime, timedelta
import lynx.kite
import warnings
from test_dag_creation import create_complex_test_workspace


class TestAirflowDagGeneration(unittest.TestCase):

  def test_trivial_dag(self):
    # We suppress deprecation warnings coming from Airflow
    warnings.simplefilter("ignore")
    lk = lynx.kite.LynxKite()

    @lk.workspace()
    def trivial():
      return dict(result=lk.createExampleGraph().sql('select name, age from vertices'))

    wss = lynx.kite.WorkspaceSequence(
        ws=trivial,
        schedule='* * * * *',
        start_date=datetime(2018, 5, 10),
        params={},
        lk_root='generated_airflow_dag_test',
        dfs_root='',
        input_recipes=[])
    trivial_eg_dag = wss.to_airflow_DAG('trivial_eg_dag')
    self.assertEqual(trivial_eg_dag.schedule_interval, '* * * * *')
    self.assertEqual(trivial_eg_dag.dag_id, 'trivial_eg_dag')
    self.assertEqual(trivial_eg_dag.owner, 'airflow')
    deps = {}
    for task in trivial_eg_dag.tasks:
      deps[task.task_id] = dict(
          upstream=[t.task_id for t in task.upstream_list],
          downstream=[t.task_id for t in task.downstream_list])
    expected = {
        'save_workspace': {
            'upstream': [],
            'downstream': ['output_result']},
        'output_result': {
            'upstream': ['save_workspace'],
            'downstream': []}
    }
    self.assertEqual(deps, expected)

  def test_compex_dag(self):
    # We suppress deprecation warnings coming from Airflow
    warnings.simplefilter("ignore")
    lk = lynx.kite.LynxKite()
    wss = lynx.kite.WorkspaceSequence(
        ws=create_complex_test_workspace(),
        schedule='0 3 * * *',
        start_date=datetime(2018, 5, 11),
        params={},
        lk_root='generated_airflow_dag_test',
        dfs_root='',
        input_recipes=[])
    complex_dag = wss.to_airflow_DAG('complex_dag')
    self.assertEqual(complex_dag.schedule_interval, '0 3 * * *')
    self.assertEqual(complex_dag.dag_id, 'complex_dag')
    self.assertEqual(complex_dag.owner, 'airflow')
    deps = {}
    for task in complex_dag.tasks:
      deps[task.task_id] = dict(
          upstream=[t.task_id for t in task.upstream_list],
          downstream=[t.task_id for t in task.downstream_list])
    expected = {
        'input_i3': {
            'upstream': [],
            'downstream': ['output_o2']},
        'input_i2': {
            'upstream': [],
            'downstream': ['snapshotter--saveToSnapshot_SB1']},
        'save_workspace': {
            'upstream': [],
            'downstream': [
                'snapshotter--saveToSnapshot_SB2',
                'output_o3',
                'snapshotter--saveToSnapshot_SB1']},
        'input_i1': {
            'upstream': [],
            'downstream': ['snapshotter--saveToSnapshot_SB2']},
        'snapshotter--saveToSnapshot_SB2': {
            'upstream': ['save_workspace', 'input_i1'],
            'downstream': ['output_o1']},
        'output_o3': {
            'upstream': ['save_workspace'],
            'downstream': []},
        'snapshotter--saveToSnapshot_SB1': {
            'upstream': ['input_i2', 'save_workspace'],
            'downstream': []},
        'output_o1': {
            'upstream': ['snapshotter--saveToSnapshot_SB2'],
            'downstream': ['output_o2']},
        'output_o2': {
            'upstream': ['input_i3', 'output_o1'],
            'downstream': []}}
    self.assertEqual(deps, expected)
