import lynx.automation
import lynx.kite
import unittest
import warnings
from datetime import datetime


class TestMagicDateIntegration(unittest.TestCase):

  def test_magicdate(self):
    # We suppress deprecation warnings coming from Airflow
    warnings.simplefilter("ignore")
    lk = lynx.kite.LynxKite()

    @lk.workspace(name='ot', parameters=[lynx.kite.text('date')])
    def trivial():
      return dict(result=lk.createExampleGraph().sql(lynx.kite.pp('select "${date.db2}"')))
    start_date = datetime(2018, 7, 8, 1, 2)
    wss = lynx.automation.WorkspaceSequence(
        ws=trivial,
        schedule='* * * * *',
        start_date=datetime(2018, 5, 10),
        lk_root='magicdate',
        input_recipes=[])

    wss_instance = wss.ws_for_date(start_date)
    wss_instance.run()
    df = wss.output_sequences['result'].read_date(start_date).df()
    self.assertEquals(df.values[0][0], "TIMESTAMP('2018-07-08 01:02:00')")
