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

  def _save_snapshots(self, lk, paths, state):
    for path in paths:
      lk.save_snapshot('test_snapshot_sequence/' + path, state)

  def _table_count(self, lk, input_table):
    state = input_table.sql1(sql='select count(1) from input')
    table_state = lk.get_state_id(state)
    table = lk.get_table(table_state)
    return table.data[0][0].double

  def test_tables_yearly(self):
    lk = lynx.kite.LynxKite()

    lk.remove_name('test_snapshot_sequence', force=True)
    lk.create_dir('test_snapshot_sequence')
    lk.create_dir('test_snapshot_sequence/1')
    outputs = lk.run(json.loads(ANCHOR_EXAMPLE_AND_SQL))
    state = outputs['SQL1_1', 'table'].stateId

    self._save_snapshots(lk, ['1/' + y for y in ['2010-01-01 00:00:00',
                                                 '2011-01-01 00:00:00', '2012-01-01 00:00:00']], state)

    tss = lynx.kite.TableSnapshotSequence(
        'test_snapshot_sequence/1',
        '0 0 1 1 *',
        datetime(2010, 1, 1, 0, 0),
        datetime(2011, 1, 1, 0, 0))
    snapshots = tss.snapshots(lk)
    self.assertEqual(len(snapshots), 2)
    self.assertEqual('test_snapshot_sequence/1/2010-01-01 00:00:00', snapshots[0])
    self.assertEqual('test_snapshot_sequence/1/2011-01-01 00:00:00', snapshots[1])
    self.assertEqual(8.0, self._table_count(lk, tss.table(lk)))

    self._save_snapshots(lk, ['2/2015-%02d-01 00:00:00' % m for m in range(1, 13)], state)
    self._save_snapshots(lk, ['2/2016-%02d-01 00:00:00' % m for m in range(1, 13)], state)

    tss = lynx.kite.TableSnapshotSequence(
        'test_snapshot_sequence/2',
        '0 0 1 * *',
        datetime(2015, 5, 1, 0, 0),
        datetime(2016, 10, 1, 0, 0))
    snapshots = tss.snapshots(lk)
    self.assertEqual(len(snapshots), 18)
    self.assertEqual('test_snapshot_sequence/2/2015-05-01 00:00:00', snapshots[0])
    self.assertEqual('test_snapshot_sequence/2/2016-10-01 00:00:00', snapshots[17])
    self.assertEqual(72.0, self._table_count(lk, tss.table(lk)))

    self._save_snapshots(lk, ['3/2017-03-%02d 00:00:00' % d for d in range(1, 32)], state)
    self._save_snapshots(lk, ['3/2017-04-%02d 00:00:00' % d for d in range(1, 31)], state)

    tss = lynx.kite.TableSnapshotSequence(
        'test_snapshot_sequence/3',
        '0 0 * * *',
        datetime(2017, 3, 15, 0, 0),
        datetime(2017, 4, 15, 0, 0))
    snapshots = tss.snapshots(lk)
    self.assertEqual(len(snapshots), 32)
    self.assertEqual('test_snapshot_sequence/3/2017-03-15 00:00:00', snapshots[0])
    self.assertEqual('test_snapshot_sequence/3/2017-04-15 00:00:00', snapshots[31])
    self.assertEqual(128.0, self._table_count(lk, tss.table(lk)))
