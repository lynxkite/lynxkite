import unittest
import lynx.kite


def stats(data_status):
  return dict(
      total_cnt=data_status.total.fileCount,
      trash_cnt=data_status.trash.fileCount,
      entities={method.id: method.fileCount for method in data_status.methods}
  )


def count_diff(before, after):
  return dict(
      total_cnt_diff=after['total_cnt'] - before['total_cnt'],
      trash_cnt_diff=after['trash_cnt'] - before['trash_cnt'],
      entities_diff={
          method: after['entities'][method] - before['entities'][method]
          for method in before['entities'].keys()}
  )


class TestCleaner(unittest.TestCase):
  data_status = None
  lk = lynx.kite.LynxKite()

  def print_info(self, msg):
    before = self.data_status
    after = stats(self.lk.get_data_files_status())
    print('== ' + msg)
    if before:
      print('Diff:', count_diff(before, after))
    else:
      print('New state:', after)
    self.data_status = after

  def test_import_output_cleaning(self):
    lk = self.lk
    self.print_info('Initial state')
    table = lk.uploadCSVNow('x,y\n1,11\n2,22')
    self.print_info('Table uploaded')
    lk.move_to_cleaner_trash('notSnapshotOrImportBoxEntities')
    self.print_info('After moving to cleaner')
    lk.empty_cleaner_trash()
    self.print_info('After running "empty cleaner"')
    print(table.df())

  def test_not_snapshot_or_workspace_entities(self):
    lk = self.lk
    lk.remove_name('cln1', force=True)
    self.print_info('Initial state')
    table = lk.createVertices(size=12).sql('select * from vertices')
    table.save_snapshot('cln1')
    self.print_info('After saving snapshot')
    table.compute()
    self.print_info('After triggering computation')
    lk.remove_name('cln1')
    self.print_info('After removing snapshot')
    lk.move_to_cleaner_trash('notSnapshotOrWorkspaceEntities')
    self.print_info('After moving to cleaner')
    lk.empty_cleaner_trash()
    self.print_info('After running "empty cleaner"')
