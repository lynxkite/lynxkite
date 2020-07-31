import unittest
import json
from datetime import datetime, timedelta, tzinfo
import dateutil.parser
import lynx.kite
from lynx.automation import utc_dt, Schedule
import pendulum

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
    "inputs": { "input": { "boxId": "eg0", "id": "graph" } },
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

    schedule = Schedule(utc_dt(2010, 1, 1), '0 0 1 1 *')
    tss = lynx.automation.TableSnapshotSequence(lk, 'test_snapshot_sequence/1', schedule)
    self._save_snapshots(tss, [utc_dt(y, 1, 1, 0, 0) for y in [
        2010, 2011, 2012]], state)

    fd = utc_dt(2010, 1, 1, 0, 0)
    td = utc_dt(2011, 1, 1, 0, 0)
    snapshots = tss._snapshots(fd, td)
    self.assertEqual(len(snapshots), 2)
    self.assertEqual('test_snapshot_sequence/1/2010-01-01T00:00:00+00:00', snapshots[0])
    self.assertEqual('test_snapshot_sequence/1/2011-01-01T00:00:00+00:00', snapshots[1])
    self.assertEqual(8.0, self._table_count(tss.read_interval(fd, td)))

    schedule = Schedule(utc_dt(2010, 1, 1), '0 0 1 * *')
    tss = lynx.automation.TableSnapshotSequence(lk, 'test_snapshot_sequence/2', schedule)
    self._save_snapshots(tss,
                         [utc_dt(2015, m, 1, 0, 0) for m in range(1, 13)] +
                         [utc_dt(2016, m, 1, 0, 0) for m in range(1, 13)], state)

    fd = utc_dt(2015, 5, 1, 0, 0)
    td = utc_dt(2016, 10, 1, 0, 0)
    snapshots = tss._snapshots(fd, td)
    self.assertEqual(len(snapshots), 18)
    self.assertEqual('test_snapshot_sequence/2/2015-05-01T00:00:00+00:00', snapshots[0])
    self.assertEqual('test_snapshot_sequence/2/2016-10-01T00:00:00+00:00', snapshots[17])
    self.assertEqual(72.0, self._table_count(tss.read_interval(fd, td)))

    schedule = Schedule(utc_dt(2010, 1, 1), '0 0 * * *')
    tss = lynx.automation.TableSnapshotSequence(lk, 'test_snapshot_sequence/3', schedule)
    self._save_snapshots(tss,
                         [utc_dt(2017, 3, d, 0, 0) for d in range(1, 32)] +
                         [utc_dt(2017, 4, d, 0, 0) for d in range(1, 31)], state)

    fd = utc_dt(2017, 3, 15, 0, 0)
    td = utc_dt(2017, 4, 15, 0, 0)
    snapshots = tss._snapshots(fd, td)
    self.assertEqual(len(snapshots), 32)
    self.assertEqual('test_snapshot_sequence/3/2017-03-15T00:00:00+00:00', snapshots[0])
    self.assertEqual('test_snapshot_sequence/3/2017-04-15T00:00:00+00:00', snapshots[31])
    self.assertEqual(128.0, self._table_count(tss.read_interval(fd, td)))

  def test_invalid_save_to_sequence(self):
    lk = lynx.kite.LynxKite()
    state = self._get_state(lk)

    schedule = Schedule(utc_dt(2010, 1, 1), '0 0 1 * *')
    tss = lynx.automation.TableSnapshotSequence(lk, 'test_snapshot_sequence/4', schedule)
    self.assertRaises(AssertionError, tss.save_to_sequence, state,
                      utc_dt(2015, 6, 15, 0, 0))

  def test_utc_snapshot_names(self):
    lk = lynx.kite.LynxKite()

    def name_to_time(name):
      return name.split('/')[-1]

    budapest = pendulum.timezone('Europe/Budapest')
    schedule = Schedule(pendulum.create(2010, 1, 1, tz=budapest), '30 1 * * *')
    tss = lynx.automation.TableSnapshotSequence(lk, 'test_snapshot_sequence/6', schedule)
    fd = utc_dt(2018, 1, 1, 0, 0)
    td = utc_dt(2018, 2, 1, 0, 0)
    snapshots = tss._snapshots(fd, td)
    local_names = [name_to_time(name) for name in snapshots]
    self.assertEqual(len(snapshots), 31)
    self.assertEqual(local_names[0], '2018-01-01T00:30:00+00:00')
    self.assertEqual(local_names[1], '2018-01-02T00:30:00+00:00')
    self.assertEqual(local_names[2], '2018-01-03T00:30:00+00:00')
    self.assertEqual(local_names[30], '2018-01-31T00:30:00+00:00')

  def test_delete_expired(self):
    lk = lynx.kite.LynxKite()
    schedule = Schedule(utc_dt(2010, 1, 1), '30 1 * * *')
    tss = lynx.automation.TableSnapshotSequence(
        lk, 'test_snapshot_sequence/7', schedule, timedelta(days=10))
    state = lk.get_state_id(lk.createExampleGraph().sql('select * from vertices'))
    tss.save_to_sequence(state, utc_dt(2018, 1, 1, 1, 30))
    self.assertEqual(1, len(lk.list_dir('test_snapshot_sequence/7')))
    tss.delete_expired(utc_dt(2018, 2, 1, 1, 30))
    self.assertEqual(0, len(lk.list_dir('test_snapshot_sequence/7')))

  def test_lazy_tss(self):
    lk = lynx.kite.LynxKite()
    date_format = '%Y-%m-%d'

    class LazyTableSnapshotSequence(lynx.automation.TableSnapshotSequence):
      def create_state_if_available(self, date):
        if date < utc_dt(5800, 1, 1):
          return lk.createExampleGraph().sql(f'select "{date: {date_format}}" as queried')

    date1 = utc_dt(5799, 11, 28)
    date2 = utc_dt(5799, 11, 29)
    date_to_fail = utc_dt(6000, 1, 1)

    schedule = Schedule(utc_dt(2010, 1, 1), '0 0 * * *')
    lazy_tss = LazyTableSnapshotSequence(lk, 'test_lazy_snapshot_sequence', schedule)
    queried = lazy_tss.read_date(date1)
    query_date = queried.get_table_data().data[0][0].string
    self.assertEqual(query_date, f'{date1: {date_format}}')
    # Should be able to handle that date1 is already done.
    queried_interval = lazy_tss.read_interval(date1, date2)
    query_dates = sorted([i[0].string for i in queried_interval.get_table_data().data])
    expected = [f'{d: {date_format}}' for d in (date1, date2)]
    self.assertListEqual(query_dates, expected)

    # If we don't provide a state for the given date then it should return a box with error.
    non_existing_snapshot = lazy_tss.read_date(date_to_fail)
    with self.assertRaises(lynx.kite.LynxException) as exc:
      data = non_existing_snapshot.get_table_data()
    error_msg = exc.exception.args[0]
    self.assertIn('does not exist', error_msg)

  def test_using_non_utc_date(self):
    lk = lynx.kite.LynxKite()
    budapest = pendulum.timezone('Europe/Budapest')
    schedule = Schedule(utc_dt(2010, 1, 1), '30 1 * * *')
    tss = lynx.automation.TableSnapshotSequence(lk, 'test_snapshot_sequence/8', schedule)
    state = lk.get_state_id(lk.createExampleGraph().sql('select age from vertices'))
    tss.save_to_sequence(state, utc_dt(2018, 1, 1, 1, 30))
    data = tss.read_date(pendulum.create(2018, 1, 1, 2, 30, tz=budapest)).get_table_data().data
    self.assertEqual([[x.string for x in row][0] for row in data], ['20.3', '18.2', '50.3', '2'])
