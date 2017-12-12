import unittest
import lynx.kite
import json
import test_workspace


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

  def test_tables_yearly(self):
    lk = lynx.kite.LynxKite()

    lk.remove_name('test_snapshot_sequence', force=True)
    lk.create_dir('test_snapshot_sequence')
    lk.create_dir('test_snapshot_sequence/1')
    outputs = lk.run(json.loads(ANCHOR_EXAMPLE_AND_SQL))
    state = outputs['SQL1_1', 'table'].stateId

    self._save_snapshots(lk, ['1/' + y for y in ['2010', '2011', '2012']], state)

    tss = lynx.kite.TableSnapshotSequence('test_snapshot_sequence/1')
    tables = tss.tables_yearly(lk, '2010', '2011')
    self.assertEqual(len(tables), 2)
    self.assertEqual('test_snapshot_sequence/1/2010', tables[0].name)
    self.assertEqual('test_snapshot_sequence/1/2011', tables[1].name)
    self.assertEqual('table', tables[0].icon)
    self.assertEqual('table', tables[1].icon)

    self._save_snapshots(lk, ['2/2015/' + '%02d' % m for m in range(1, 13)], state)
    self._save_snapshots(lk, ['2/2016/' + '%02d' % m for m in range(1, 13)], state)

    tss = lynx.kite.TableSnapshotSequence('test_snapshot_sequence/2')
    tables = tss.tables_monthly(lk, '2015/05', '2016/10')
    self.assertEqual(len(tables), 18)
    self.assertEqual('test_snapshot_sequence/2/2015/05', tables[0].name)
    self.assertEqual('test_snapshot_sequence/2/2016/10', tables[17].name)
    self.assertEqual('table', tables[0].icon)
    self.assertEqual('table', tables[1].icon)

    self._save_snapshots(lk, ['3/2017/03/' + '%02d' % d for d in range(1, 32)], state)
    self._save_snapshots(lk, ['3/2017/04/' + '%02d' % d for d in range(1, 31)], state)

    tss = lynx.kite.TableSnapshotSequence('test_snapshot_sequence/3')
    tables = tss.tables_daily(lk, '2017/03/15', '2017/04/15')
    self.assertEqual(len(tables), 32)
    self.assertEqual('test_snapshot_sequence/3/2017/03/15', tables[0].name)
    self.assertEqual('test_snapshot_sequence/3/2017/04/15', tables[31].name)
    self.assertEqual('table', tables[0].icon)
    self.assertEqual('table', tables[1].icon)
