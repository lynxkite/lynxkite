import unittest
import lynx.kite
from lynx.kite import pp, text
import lynx.automation
from lynx.automation import _aware_to_iso_str
from lynx.automation import Schedule, utc_dt
from datetime import datetime, timedelta


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

    test_date = utc_dt(2018, 1, 2)
    schedule = Schedule(utc_dt(2018, 1, 1), '0 0 * * *')
    tss = lynx.automation.TableSnapshotSequence(lk, 'eg_table_seq', schedule)
    lk.createExampleGraph().sql('select * from vertices').save_to_sequence(tss, test_date)
    input_recipe = lynx.automation.TableSnapshotRecipe(tss)
    wss = lynx.automation.WorkspaceSequence(
        ws=builder,
        schedule=schedule,
        # Test if trailing slash is duplicated or not.
        lk_root='ws_test_seq/',
        input_recipes=[input_recipe])
    wss_instance = wss.wssi_for_date(test_date)
    wss_instance.run()
    for output_sequence in wss.output_sequences.values():
      self.assertTrue(lynx.automation.TableSnapshotRecipe(output_sequence).is_ready(test_date))
    cnt_result_tss = wss.output_sequences['cnt']
    table_raw = cnt_result_tss.read_interval(test_date, test_date).get_table_data()
    self.assertEqual(table_raw.data[0][0].string, '4')
    d_result_tss = wss.output_sequences['d']
    table_raw = d_result_tss.read_interval(test_date, test_date).get_table_data()
    self.assertEqual(table_raw.data[0][0].string, '2018-01-02T00:00:00+00:00')
    early_date = utc_dt(2017, 12, 31)
    with self.assertRaises(Exception) as context:
      early_instance = wss.wssi_for_date(early_date)
    self.assertTrue('preceeds start date' in str(context.exception))

  def test_multiple_save(self):
    lk = lynx.kite.LynxKite()
    lk.remove_name('eg_cnt_seq', force=True)
    lk.remove_name('eg_cnt', force=True)

    @lk.workspace(name='counter')
    def builder(table):
      o1 = table.sql('select count(*) as cnt from input')
      return dict(cnt=o1)

    test_date = utc_dt(2018, 1, 2)
    schedule = Schedule(utc_dt(2018, 1, 1), '0 0 * * *')
    tss = lynx.automation.TableSnapshotSequence(lk, 'eg_cnt_seq', schedule)
    lk.createExampleGraph().sql('select * from vertices').save_to_sequence(tss, test_date)
    input_recipe = lynx.automation.TableSnapshotRecipe(tss)
    wss = lynx.automation.WorkspaceSequence(
        ws=builder,
        schedule=schedule,
        lk_root='eg_cnt',
        input_recipes=[input_recipe])
    wss_instance = wss.wssi_for_date(test_date)
    wss_instance.save()
    # The second save overwrites this first
    wss_instance.save()

  def test_input_depends_on_output(self):
    lk = lynx.kite.LynxKite()
    lk.remove_name('ws_test_seq_2', force=True)

    @lk.workspace(name='sequence_2')
    def builder(table):
      o = table.sql('select summa * 2 as summa from input')
      return dict(summa=o)

    initial_state = lk.createExampleGraph().sql('select count(1) as summa from vertices')
    summa_as_input = lynx.automation.TableSnapshotRecipe(None, delta=1)
    summa_with_default = lynx.automation.RecipeWithDefault(
        summa_as_input, utc_dt(2018, 1, 1), initial_state)
    schedule = Schedule(utc_dt(2018, 1, 1), '0 0 * * *')
    wss = lynx.automation.WorkspaceSequence(
        ws=builder,
        schedule=schedule,
        lk_root='ws_test_seq_2',
        input_recipes=[summa_with_default])
    summa_as_input.set_tss(wss.output_sequences['summa'])

    def run_ws(test_date, summa):
      wss_instance = wss.wssi_for_date(test_date)
      wss_instance.run()
      for output_sequence in wss.output_sequences.values():
        self.assertTrue(lynx.automation.TableSnapshotRecipe(
            output_sequence).is_ready(test_date))
      summa_result_tss = wss.output_sequences['summa']
      table_raw = summa_result_tss.read_interval(test_date, test_date).get_table_data()
      self.assertEqual(table_raw.data[0][0].string, str(summa))

    run_ws(utc_dt(2018, 1, 1), '8')
    run_ws(utc_dt(2018, 1, 2), '16')
    run_ws(utc_dt(2018, 1, 3), '32')
    run_ws(utc_dt(2018, 1, 4), '64')

  def test_side_effects_in_sequence(self):
    lk = lynx.kite.LynxKite()
    lk.remove_name('ws_test_seq_3', force=True)
    lk.remove_name('wsi_snapshots', force=True)

    @lk.workspace_with_side_effects(name='eg_stats', parameters=[text('date')])
    def builder(sec):
      eg = lk.createExampleGraph()
      o = eg.sql('select avg(age) as avg from vertices')
      (eg.sql(pp('select income, "${date}" as d from vertices'))
         .saveToSnapshot(path=pp('wsi_snapshots/${date}'))
         .register(sec))
      return dict(avg=o)

    schedule = Schedule(utc_dt(2018, 4, 5), '0 0 * * *')
    wss = lynx.automation.WorkspaceSequence(
        ws=builder,
        schedule=schedule,
        lk_root='ws_test_seq_3',
        input_recipes=[])

    def run_ws(test_date):
      wss_instance = wss.wssi_for_date(test_date)
      wss_instance.run()
      for output_sequence in wss.output_sequences.values():
        self.assertTrue(lynx.automation.TableSnapshotRecipe(
            output_sequence).is_ready(test_date))
      avg_result_tss = wss.output_sequences['avg']
      table_raw = avg_result_tss.read_interval(test_date, test_date).get_table_data()
      self.assertEqual(table_raw.data[0][0].string, '22.7')
      entries = lk.list_dir('wsi_snapshots')
      self.assertTrue(f'wsi_snapshots/{_aware_to_iso_str(test_date)}' in [e.name for e in entries])

    run_ws(utc_dt(2018, 4, 5))
    run_ws(utc_dt(2018, 4, 6))
    run_ws(utc_dt(2018, 4, 7))

  def test_output_retention(self):
    def not_deleted():
      return len([s.name
                  for s in lk.list_dir(wss.output_sequences['cnt']._location)])

    lk = lynx.kite.LynxKite()
    lk.remove_name('wss_retention', force=True)
    lk.remove_name('wss_retention_seq', force=True)

    @lk.workspace(name='counter')
    def builder(table):
      o1 = table.sql('select count(*) as cnt from input')
      return dict(cnt=o1)

    test_days = [utc_dt(2018, 1, 1) + timedelta(days=x) for x in range(0, 10)]
    schedule = Schedule(utc_dt(2018, 1, 1), '0 0 * * *')
    tss = lynx.automation.TableSnapshotSequence(lk, 'wss_retention_seq', schedule)
    for day in test_days:
      lk.createExampleGraph().sql('select * from vertices').save_to_sequence(tss, day)
    input_recipe = lynx.automation.TableSnapshotRecipe(tss)
    wss = lynx.automation.WorkspaceSequence(
        ws=builder,
        schedule=schedule,
        lk_root='wss_retention',
        input_recipes=[input_recipe],
        retention_deltas=dict(cnt=timedelta(days=3)))
    # In the end, we have 1 + retention days snapshots.
    expected_sn_count = [1, 2, 3, 4, 4, 4, 4, 4, 4, 4]
    for i, day in enumerate(test_days):
      wss.run_dag_tasks(day)
      self.assertEqual(not_deleted(), expected_sn_count[i])
