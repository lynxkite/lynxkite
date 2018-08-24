import unittest
from datetime import datetime, timedelta
import lynx.kite
import lynx.automation
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
      return dict(result=lk.createExampleGraph().sql('select ${date.db2()} from vertices'))

    wss = lynx.automation.WorkspaceSequence(
        ws=trivial,
        schedule='* * * * *',
        start_date=datetime(2018, 5, 10),
        lk_root='generated_airflow_dag_test',
        input_recipes=[])
    trivial_eg_dag = wss.to_airflow_DAG('trivial_eg_dag')
    d = datetime(2018, 7, 7)
    trivial_eg_dag.run(start_date=d)
    # HELP
