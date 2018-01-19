import unittest
import lynx.kite
import json
import test_workspace
from datetime import datetime

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

  def _save_snapshots(self, lk, tss, datetimes, state):
    for datetime in datetimes:
      tss.save_to_sequence(lk, state, datetime)

  def _table_count(self, lk, input_table):
    table = input_table.sql1(sql='select count(1) from input').get_table_data(lk)
    return table.data[0][0].double

  def _get_state(self, lk):
    lk.remove_name('test_snapshot_sequence', force=True)
    lk.create_dir('test_snapshot_sequence')
    outputs = lk.run(json.loads(ANCHOR_EXAMPLE_AND_SQL))
    return outputs['SQL1_1', 'table'].stateId

  def test_read_interval(self):
    lk = lynx.kite.LynxKite()
    state = self._get_state(lk)

    tss = lynx.kite.TableSnapshotSequence('test_snapshot_sequence/1', '0 0 1 1 *')
    self._save_snapshots(lk, tss, [datetime(y, 1, 1, 0, 0) for y in [2010, 2011, 2012]], state)

    fd = datetime(2010, 1, 1, 0, 0)
    td = datetime(2011, 1, 1, 0, 0)
    snapshots = tss.snapshots(lk, fd, td)
    self.assertEqual(len(snapshots), 2)
    self.assertEqual('test_snapshot_sequence/1/2010-01-01 00:00:00', snapshots[0])
    self.assertEqual('test_snapshot_sequence/1/2011-01-01 00:00:00', snapshots[1])
    self.assertEqual(8.0, self._table_count(lk, tss.read_interval(lk, fd, td)))

    tss = lynx.kite.TableSnapshotSequence('test_snapshot_sequence/2', '0 0 1 * *')
    self._save_snapshots(lk, tss,
                         [datetime(2015, m, 1, 0, 0) for m in range(1, 13)] +
                         [datetime(2016, m, 1, 0, 0) for m in range(1, 13)], state)

    fd = datetime(2015, 5, 1, 0, 0)
    td = datetime(2016, 10, 1, 0, 0)
    snapshots = tss.snapshots(lk, fd, td)
    self.assertEqual(len(snapshots), 18)
    self.assertEqual('test_snapshot_sequence/2/2015-05-01 00:00:00', snapshots[0])
    self.assertEqual('test_snapshot_sequence/2/2016-10-01 00:00:00', snapshots[17])
    self.assertEqual(72.0, self._table_count(lk, tss.read_interval(lk, fd, td)))

    tss = lynx.kite.TableSnapshotSequence('test_snapshot_sequence/3', '0 0 * * *')
    self._save_snapshots(lk, tss,
                         [datetime(2017, 3, d, 0, 0) for d in range(1, 32)] +
                         [datetime(2017, 4, d, 0, 0) for d in range(1, 31)], state)

    fd = datetime(2017, 3, 15, 0, 0)
    td = datetime(2017, 4, 15, 0, 0)
    snapshots = tss.snapshots(lk, fd, td)
    self.assertEqual(len(snapshots), 32)
    self.assertEqual('test_snapshot_sequence/3/2017-03-15 00:00:00', snapshots[0])
    self.assertEqual('test_snapshot_sequence/3/2017-04-15 00:00:00', snapshots[31])
    self.assertEqual(128.0, self._table_count(lk, tss.read_interval(lk, fd, td)))

  def test_invalid_save_to_sequence(self):
    lk = lynx.kite.LynxKite()
    state = self._get_state(lk)

    tss = lynx.kite.TableSnapshotSequence('test_snapshot_sequence/4', '0 0 1 * *')
    self.assertRaises(AssertionError, tss.save_to_sequence, lk, state, datetime(2015, 6, 15, 0, 0))
