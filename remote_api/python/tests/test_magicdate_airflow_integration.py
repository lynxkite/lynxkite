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

    @lk.workspace(name='ot', parameters=[lynx.kite.text('date')])
    def trivial(table):
      return dict(result=table.sql(lynx.kite.pp('select "${date.db2()}"')))
    start_date = datetime(2018, 7, 8, 1, 2)
    tss = lynx.kite.TableSnapshotSequence(lk, 'eg_table_seq', '* * * * *')
    lk.createExampleGraph().sql('select * from vertices').save_to_sequence(tss, start_date)
    input_recipe = lynx.automation.TableSnapshotRecipe(tss)
    wss = lynx.automation.WorkspaceSequence(
        ws=trivial,
        schedule='* * * * *',
        start_date=datetime(2018, 5, 10),
        lk_root='ete_airflow_dag_test',
        input_recipes=[input_recipe])

    wss_instance = wss.ws_for_date(start_date)
    wss_instance.run()
    df = wss.output_sequences['result'].read_date(start_date).df()
    self.assertEquals(df.values[0][0], "TIMESTAMP('2018-07-08 01:02:00')")
