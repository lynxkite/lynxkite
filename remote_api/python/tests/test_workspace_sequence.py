import unittest
import lynx.kite
from lynx.kite import pp, text
from datetime import datetime


class TestWorkspaceSequence(unittest.TestCase):

  def test_one_date(self):
    lk = lynx.kite.LynxKite()
    lk.remove_name('eg_table_seq', force=True)
    lk.remove_name('ws_test_seq', force=True)

    @lk.workspace(name='sequence', parameters=[text('date')])
    def builder(table):
      o1 = table.sql('select count(*) as cnt from input')
      o2 = table.sql(pp('select "${date}" as d from input'))
      return dict(cnt=o1, d=o2)

    test_date = datetime(2018, 1, 2)
    tss = lynx.kite.TableSnapshotSequence('eg_table_seq', '0 0 * * *')
    table_state = lk.get_state_id(lk.createExampleGraph().sql('select * from vertices'))
    tss.save_to_sequence(lk, table_state, test_date)
    input_recipe = lynx.kite.TableSnapshotRecipe(tss)
    wss = lynx.kite.WorkspaceSequence(
        ws=builder,
        schedule='0 0 * * *',
        start_date=datetime(2018, 1, 1),
        params={},
        lk_root='ws_test_seq/',
        dfs_root='',
        input_recipes=[input_recipe])
    wss_instance = wss.ws_for_date(lk, test_date)
    wss_instance.save()
    wss_instance.run()
    for output_sequence in wss.output_sequences().values():
      self.assertTrue(lynx.kite.TableSnapshotRecipe(output_sequence).is_ready(lk, test_date))
    cnt_result_tss = wss.output_sequences()['cnt']
    table_state = lk.get_state_id(cnt_result_tss.read_interval(lk, test_date, test_date))
    table_raw = lk.get_table(table_state)
    self.assertEqual(table_raw.data[0][0].string, '4')
    d_result_tss = wss.output_sequences()['d']
    table_state = lk.get_state_id(d_result_tss.read_interval(lk, test_date, test_date))
    table_raw = lk.get_table(table_state)
    self.assertEqual(table_raw.data[0][0].string, '2018-01-02 00:00:00')
    early_date = datetime(2017, 12, 31)
    with self.assertRaises(Exception) as context:
      early_instance = wss.ws_for_date(lk, early_date)
    self.assertTrue('preceeds start date' in str(context.exception))
