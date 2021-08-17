import unittest
import lynx.kite


def stats(data_status):
  return dict(
      total_cnt=data_status.total.fileCount,
      trash_cnt=data_status.trash.fileCount,
      entities={method.id: method.fileCount for method in data_status.methods}
  )


def simpler(status):
  return {**status['entities'], **status}


class TestCleaner(unittest.TestCase):
  data_status = None
  lk = lynx.kite.LynxKite()
  original_cleaner_min_age = lk.get_data_files_status().cleanerMinAgeDays

  @classmethod
  def setUpClass(cls):
    cls.lk.set_cleaner_min_age(days=-1)

  @classmethod
  def tearDownClass(cls):
    cls.lk.set_cleaner_min_age(days=cls.original_cleaner_min_age)

  def check(self, msg, expected=None):
    '''The exact number of files and entities are not always predictable, so
    the test asserts on the sign of the changes. E.g. when we create
    a new snapshot, we know that the number of files has to increase, and
    when we delete a snapshot, which referred to computed entitties,
    we know that the number of "notSnapshotEntities" has to increase.'''
    before = self.data_status
    after = simpler(stats(self.lk.get_data_files_status()))
    if before:
      print(f'  Diff({msg})')
      if expected:
        for key in expected.keys():
          if expected[key] == '+':
            self.assertGreater(after[key], before[key])
          elif expected[key] == '0':
            self.assertEqual(after[key], before[key])
          elif expected[key] == '-':
            self.assertLess(after[key], before[key])
          else:
            raise Exception('Wrong sign')
    else:
      print(f'Initial state({msg})')
    self.data_status = after

  def test_not_snapshot_cleaning(self):
    lk = self.lk
    self.data_status = None
    self.check('Not snapshot cleaning')
    lk.remove_name('cln1', force=True)
    table = lk.uploadCSVNow('x,y,z\n1,11,111\n2,22,222')
    table.save_snapshot('cln1')
    self.check('Snapshot created', {'total_cnt': '+'})
    lk.remove_name('cln1')
    self.check('Snapshot deleted', {'notSnapshotEntities': '+'})
    lk.move_to_cleaner_trash('notSnapshotEntities')
    self.check('After moving to cleaner', {
        'total_cnt': '-',
        'trash_cnt': '+',
        'notSnapshotEntities': '-'})
    lk.empty_cleaner_trash()
    self.check('After running "empty cleaner"', {'trash_cnt': '-'})

  def test_snapshot_is_working_after_cleaning(self):
    lk = self.lk
    lk.remove_name('saved_snapshot_cln', force=True)
    table = lk.uploadCSVNow('id,amount\n1,100\n2,320')
    table.save_snapshot('saved_snapshot_cln')
    lk.move_to_cleaner_trash('notSnapshotEntities')
    lk.empty_cleaner_trash()
    table = lk.importSnapshot(path='saved_snapshot_cln').get_table_data()
    data = [[x.string for x in row] for row in table.data]
    expected = [['1', '100'], ['2', '320']]
    self.assertEqual(data, expected)

  def test_import_output_cleaning(self):
    lk = self.lk
    self.data_status = None
    self.check('Import output cleaning')
    table = lk.uploadCSVNow('x,y\n1,11\n2,22')
    table.df()
    self.check('Table uploaded', {'total_cnt': '+'})
    lk.move_to_cleaner_trash('notSnapshotOrImportBoxEntities')
    self.check('After moving to cleaner', {
        'total_cnt': '-',
        'trash_cnt': '+'})
    lk.empty_cleaner_trash()
    self.check('After running "empty cleaner"', {'trash_cnt': '-'})

  def test_imported_table_is_working_after_cleaning(self):
    lk = self.lk
    table = lk.uploadCSVNow('id,sum\n1,400\n2,550')
    lk.move_to_cleaner_trash('notSnapshotOrImportBoxEntities')
    lk.empty_cleaner_trash()
    data = [[x.string for x in row] for row in table.get_table_data().data]
    expected = [['1', '400'], ['2', '550']]
    self.assertEqual(data, expected)

  def test_not_snapshot_or_workspace_entities(self):
    lk = self.lk
    self.data_status = None
    lk.remove_name('cln2', force=True)
    self.check('Not snapshot or workspace cleaning')
    table = lk.createVertices(size=12).sql('select * from vertices')
    table.save_snapshot('cln2')
    self.check('After saving snapshot', {
        'total_cnt': '0',
        'trash_cnt': '0',
        'notSnapshotOrWorkspaceEntities': '0'})
    table.compute()
    self.check('After triggering computation', {
        'total_cnt': '+',
        'trash_cnt': '0',
        'notSnapshotEntities': '+'})
    lk.remove_name('cln2')
    self.check('After removing snapshot', {
        'total_cnt': '0',
        'trash_cnt': '0',
        'notSnapshotEntities': '+'})
    lk.move_to_cleaner_trash('notSnapshotOrWorkspaceEntities')
    self.check('After moving to cleaner', {
        'total_cnt': '-',
        'trash_cnt': '+',
        'notSnapshotEntities': '-'})
    lk.empty_cleaner_trash()
    self.check('After running "empty cleaner"', {'trash_cnt': '-'})
