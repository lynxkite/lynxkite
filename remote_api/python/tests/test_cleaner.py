import unittest
import lynx.kite


def stats(data_status):
  return dict(
      total_cnt=data_status.total.fileCount,
      trash_cnt=data_status.trash.fileCount,
      entities={method.id: method.fileCount for method in data_status.methods}
  )


class TestCleaner(unittest.TestCase):

  def test_read_interval(self):
    lk = lynx.kite.LynxKite()
    print(stats(lk.get_data_files_status()))
    lk.createVertices(size=123).sql('select * from vertices').save_snapshot('cln1')
    print(stats(lk.get_data_files_status()))
    lk.createVertices(size=123).sql('select * from vertices').compute()
    print(stats(lk.get_data_files_status()))
    lk.remove_name('cln1')
    print(stats(lk.get_data_files_status()))
    lk.move_to_cleaner_trash('notSnapshotOrWorkspaceEntities')
    print(stats(lk.get_data_files_status()))
    lk.empty_cleaner_trash()
    print(stats(lk.get_data_files_status()))
