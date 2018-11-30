import unittest
import json
from datetime import datetime, timedelta, timezone, tzinfo
import dateutil.parser
import lynx.kite

ANCHOR_EXAMPLE_AND_SQL = '''
  [{
    "id": "anchor",
    "operationId": "Anchor",
    "parameters": {},
    "x": 0, "y": 0,
    "inputs": {},
    "parametricParameters": {}
  }, {
    "id": "eg0",
    "operationId": "Create example graph",
    "parameters": {},
    "x": 100, "y": 0,
    "inputs": {},
    "parametricParameters": {}
  }, {
    "id": "SQL1_1",
    "operationId": "SQL1",
    "parameters": {
      "sql": "select * from `vertices`"
    },
    "x": 200, "y": 0,
    "inputs": { "input": { "boxId": "eg0", "id": "project" } },
    "parametricParameters": {}
  }]'''


class TestSnapshotSequence(unittest.TestCase):

  def _save_snapshots(self, tss, datetimes, state):
    for dt in datetimes:
      tss.save_to_sequence(state, dt)

  def _table_count(self, input_table):
    table = input_table.sql1(sql='select count(1) from input').get_table_data()
    return table.data[0][0].double

  def _get_state(self, lk):
    lk.remove_name('test_snapshot_sequence', force=True)
    lk.create_dir('test_snapshot_sequence')
    outputs = lk.fetch_states(json.loads(ANCHOR_EXAMPLE_AND_SQL))
    return outputs['SQL1_1', 'table'].stateId

  def test_read_interval(self):
    lk = lynx.kite.LynxKite()
    state = self._get_state(lk)

    tss = lynx.kite.TableSnapshotSequence(lk, 'test_snapshot_sequence/1', '0 0 1 1 *')
    self._save_snapshots(tss, [datetime(y, 1, 1, 0, 0, tzinfo=timezone.utc) for y in [
        2010, 2011, 2012]], state)

    fd = datetime(2010, 1, 1, 0, 0, tzinfo=timezone.utc)
    td = datetime(2011, 1, 1, 0, 0, tzinfo=timezone.utc)
    snapshots = tss._snapshots(fd, td)
    self.assertEqual(len(snapshots), 2)
    self.assertEqual('test_snapshot_sequence/1/2010-01-01 00:00:00+00:00', snapshots[0])
    self.assertEqual('test_snapshot_sequence/1/2011-01-01 00:00:00+00:00', snapshots[1])
    self.assertEqual(8.0, self._table_count(tss.read_interval(fd, td)))

    tss = lynx.kite.TableSnapshotSequence(lk, 'test_snapshot_sequence/2', '0 0 1 * *')
    self._save_snapshots(tss,
                         [datetime(2015, m, 1, 0, 0, tzinfo=timezone.utc) for m in range(1, 13)] +
                         [datetime(2016, m, 1, 0, 0, tzinfo=timezone.utc) for m in range(1, 13)], state)

    fd = datetime(2015, 5, 1, 0, 0, tzinfo=timezone.utc)
    td = datetime(2016, 10, 1, 0, 0, tzinfo=timezone.utc)
    snapshots = tss._snapshots(fd, td)
    self.assertEqual(len(snapshots), 18)
    self.assertEqual('test_snapshot_sequence/2/2015-05-01 00:00:00+00:00', snapshots[0])
    self.assertEqual('test_snapshot_sequence/2/2016-10-01 00:00:00+00:00', snapshots[17])
    self.assertEqual(72.0, self._table_count(tss.read_interval(fd, td)))

    tss = lynx.kite.TableSnapshotSequence(lk, 'test_snapshot_sequence/3', '0 0 * * *')
    self._save_snapshots(tss,
                         [datetime(2017, 3, d, 0, 0, tzinfo=timezone.utc) for d in range(1, 32)] +
                         [datetime(2017, 4, d, 0, 0, tzinfo=timezone.utc) for d in range(1, 31)], state)

    fd = datetime(2017, 3, 15, 0, 0, tzinfo=timezone.utc)
    td = datetime(2017, 4, 15, 0, 0, tzinfo=timezone.utc)
    snapshots = tss._snapshots(fd, td)
    self.assertEqual(len(snapshots), 32)
    self.assertEqual('test_snapshot_sequence/3/2017-03-15 00:00:00+00:00', snapshots[0])
    self.assertEqual('test_snapshot_sequence/3/2017-04-15 00:00:00+00:00', snapshots[31])
    self.assertEqual(128.0, self._table_count(tss.read_interval(fd, td)))

  def test_invalid_save_to_sequence(self):
    lk = lynx.kite.LynxKite()
    state = self._get_state(lk)

    tss = lynx.kite.TableSnapshotSequence(lk, 'test_snapshot_sequence/4', '0 0 1 * *')
    self.assertRaises(AssertionError, tss.save_to_sequence, state,
                      datetime(2015, 6, 15, 0, 0, tzinfo=timezone.utc))

  def test_different_timezone(self):
    lk = lynx.kite.LynxKite()
    state = self._get_state(lk)

    tss = lynx.kite.TableSnapshotSequence(lk, 'test_snapshot_sequence/5', '0 0 1 1 *')
    self._save_snapshots(
        tss, [
            datetime(
                y, 1, 1, 0, 0, tzinfo=timezone.utc) for y in [
                2010, 2011, 2012]], state)

    class UTCPlus3Hours(tzinfo):
      def utcoffset(self, dt):
        return timedelta(hours=3) + self.dst(dt)

      def dst(self, dt):
        return timedelta(hours=0)

    fd = datetime(2010, 1, 1, 3, 0, tzinfo=UTCPlus3Hours())
    td = datetime(2011, 1, 1, 3, 0, tzinfo=UTCPlus3Hours())
    snapshots = tss._snapshots(fd, td)
    self.assertEqual(len(snapshots), 1)
    self.assertEqual('test_snapshot_sequence/5/2010-12-31 21:00:00+00:00', snapshots[0])

  def test_utc_snapshot_names(self):
    lk = lynx.kite.LynxKite()

    def name_to_local_time(name):
      dt = name.split('/')[-1]
      utc = dateutil.parser.parse(dt)
      local_timezone = datetime.now(timezone.utc).astimezone().tzinfo
      local_time = utc.astimezone(local_timezone)
      return local_time.strftime("%Y-%m-%d %H:%M")

    tss = lynx.kite.TableSnapshotSequence(lk, 'test_snapshot_sequence/6', '30 1 * * *')
    fd = datetime(2018, 1, 1, 0, 0)
    td = datetime(2018, 2, 1, 0, 0)
    snapshots = tss._snapshots(fd, td)
    local_names = [name_to_local_time(name) for name in snapshots]
    self.assertEqual(len(snapshots), 31)
    self.assertEqual(local_names[0], '2018-01-01 01:30')
    self.assertEqual(local_names[1], '2018-01-02 01:30')
    self.assertEqual(local_names[2], '2018-01-03 01:30')
    self.assertEqual(local_names[30], '2018-01-31 01:30')

  def test_delete_expired(self):
    lk = lynx.kite.LynxKite()
    tss = lynx.kite.TableSnapshotSequence(
        lk, 'test_snapshot_sequence/7', '30 1 * * *', timedelta(days=10))
    state = lk.get_state_id(lk.createExampleGraph().sql('select * from vertices'))
    tss.save_to_sequence(state, datetime(2018, 1, 1, 1, 30))
    self.assertEqual(1, len(lk.list_dir('test_snapshot_sequence/7')))
    tss.delete_expired(datetime(2018, 2, 1, 1, 30))
    self.assertEqual(0, len(lk.list_dir('test_snapshot_sequence/7')))

  def test_lazy_tss(self):
    lk = lynx.kite.LynxKite()
    date_format = '%Y-%m-%d'

    class LazyTableSnapshotSequence(lynx.kite.TableSnapshotSequence):
      def create_state_if_available(self, date):
        return lk.createExampleGraph().sql(f'select "{date: {date_format}}" as queried')

    date1 = datetime(5799, 11, 28)
    date2 = datetime(5799, 11, 29)

    lazy_tss = LazyTableSnapshotSequence(lk, 'test_lazy_snapshot_sequence', '0 0 * * *')
    queried = lazy_tss.read_date(date1)
    query_date = queried.get_table_data().data[0][0].string
    self.assertEqual(query_date, f'{date1: {date_format}}')
    # Should be able to handle that date1 is already done.
    queried_interval = lazy_tss.read_interval(date1, date2)
    query_dates = {i[0].string for i in queried_interval.get_table_data().data}
    expected = {f'{d: {date_format}}' for d in (date1, date2)}
    self.assertSetEqual(query_dates, expected)
