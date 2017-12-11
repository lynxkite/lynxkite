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

  def test_tables_yearly(self):
    lk = lynx.kite.LynxKite()

    lk.create_dir('test_snapshot_sequence')
    lk.create_dir('test_snapshot_sequence/1')
    outputs = lk.run(json.loads(ANCHOR_EXAMPLE_AND_SQL))
    state = outputs['SQL1_1', 'table'].stateId
    lk.save_snapshot('test_snapshot_sequence/1/2010', state)
    lk.save_snapshot('test_snapshot_sequence/1/2011', state)
    lk.save_snapshot('test_snapshot_sequence/1/2012', state)

    tss = lynx.kite.TableSnapshotSequence('test_snapshot_sequence/1')
    tables = tss.tables_yearly(lk, '2010', '2011')
    self.assertEqual(len(tables), 2)
    self.assertEqual('test_snapshot_sequence/1/2010', tables[0].name)
    self.assertEqual('test_snapshot_sequence/1/2011', tables[1].name)
    self.assertEqual('table', tables[0].icon)
    self.assertEqual('table', tables[1].icon)
