import unittest
from datetime import datetime, timedelta
import lynx.kite
import warnings


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
