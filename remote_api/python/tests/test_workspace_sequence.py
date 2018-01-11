import unittest
import lynx.kite
from lynx.kite import pp, text
from datetime import datetime


class TestWorkspaceSequence(unittest.TestCase):

  def _entry_exists(self, lk, location, name):
    entries = lk.list_dir(location)
    entry_names = [e.name for e in entries]
    return location + '/' + name in entry_names

  def test_one_date(self):
    lk = lynx.kite.LynxKite()
    lk.remove_name('eg_table_seq', force=True)
    lk.remove_name('ws_test_seq', force=True)

    @lk.workspace(name='sequence', parameters=[text('date')])
    def builder(table):
      o1 = table.sql('select count(*) as cnt from input')
      o2 = table.sql('select "${date}" as d from input')
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
    wss_instance.save(lk)
    wss_instance.run(lk)
    for output_sequence in wss.output_sequences().values():
      self.assertTrue(lynx.kite.TableSnapshotRecipe(output_sequence).is_ready(lk, test_date))
    result_tss = wss.output_sequences()['cnt']
    table_state = lk.get_state_id(result_tss.read_interval(lk, test_date, test_date))
    table_raw = lk.get_table(table_state)
    self.assertEqual(table_raw.data[0][0].string, '4')
