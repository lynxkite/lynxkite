import unittest
import lynx.kite
import json
import test_workspace


class TestSnapshotSequence(unittest.TestCase):

  def test_tables(self):
    lk = lynx.kite.LynxKite()

    # TODO: Set up test dirs and snapshots.

    tss = lynx.kite.TableSnapshotSequence('test_snapshot_sequence/1')
    tables = tss.tables_monthly(lk, '2017/11', '2017/12')
    self.assertEqual(len(tables), 2)
    self.assertEqual('test_snapshot_sequence/1/2017/11', tables[0].name)
    self.assertEqual('test_snapshot_sequence/1/2017/12', tables[1].name)
    self.assertEqual('table', tables[0].icon)
    self.assertEqual('table', tables[1].icon)
