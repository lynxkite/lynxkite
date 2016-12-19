import unittest
import lynx
from datetime import datetime


class TestViewsAndGlobalSql(unittest.TestCase):

  lk = lynx.LynxKite()
  lk._request('/ajax/discardAllReallyIMeanIt')
  p = lk.new_project()
  p.exampleGraph()

  def generate_view(self, frame=p):
    '''Used to generate views for the tests. The first test should test if it really works.'''
    return self.lk.sql('SELECT name, age FROM `p`', p=frame)

  def check_view(self, view):
    '''Used to check whether a view really points to the project p.'''
    names = sorted([i['name'] for i in view.take(4)])
    self.assertEqual(names, ['Adam', 'Bob', 'Eve', 'Isolated Joe'])

  def test_upload(self):
    csv = 'name\nAdam\nBob\nEve\nIsolated Joe'
    path = self.lk.upload(csv)
    view = self.lk.import_csv(path)
    self.check_view(view)

  def test_global_sql_with_project_input(self):
    view = self.generate_view()
    self.check_view(view)

  def test_global_sql_with_view_input(self):
    view = self.generate_view()
    view2 = self.generate_view(view)
    self.check_view(view2)

  def test_export_view_to_table(self):
    view = self.generate_view()
    table = view.to_table()
    table.save('test_table')
    table2 = self.lk.load_table('test_table')
    self.assertTrue(table2.checkpoint is not None)
    self.assertEqual(table.checkpoint, table2.checkpoint)

  def test_global_sql_with_table_input(self):
    '''It is also used to see if the table generated above really is a table containing what we want.'''
    view = self.generate_view()
    table = view.to_table()
    view2 = self.generate_view(table)
    self.check_view(view2)

  def test_save_and_load_view(self):
    view = self.generate_view()
    view.save('test_view')
    view2 = self.lk.load_view('test_view')
    self.check_view(view2)

  def test_export_view_to_csv_and_load_back(self):
    view = self.generate_view()
    date = datetime.now().strftime('%Y-%m-%d %Hh %Mm %Ss')
    path = 'DATA$/tmp/test_export_view_to_csv(' + date + ').csv'
    view.export_csv(path)
    view2 = self.lk.import_csv(path)
    self.check_view(view2)

  def test_export_view_to_parquet_and_load_back(self):
    view = self.generate_view()
    date = datetime.now().strftime('%Y-%m-%d %Hh %Mm %Ss')
    path = 'DATA$/tmp/test_export_view_to_parquet(' + date + ').parquet'
    view.export_parquet(path)
    view2 = self.lk.import_parquet(path)
    self.check_view(view2)

  def test_export_view_to_json_and_load_back(self):
    view = self.generate_view()
    date = datetime.now().strftime('%Y-%m-%d %Hh %Mm %Ss')
    path = 'DATA$/tmp/test_export_view_to_json(' + date + ').json'
    view.export_json(path)
    view2 = self.lk.import_json(path)
    self.check_view(view2)

  def test_export_view_to_orc_and_load_back(self):
    view = self.generate_view()
    # From Spark 2.0 the ORC read path cannot handle spaces in file names on local disk.
    date = datetime.now().strftime('%Y-%m-%d_%Hh_%Mm_%Ss')
    path = 'DATA$/tmp/test_export_view_to_orc(' + date + ').orc'
    view.export_orc(path)
    view2 = self.lk.import_orc(path)
    self.check_view(view2)


if __name__ == '__main__':
  unittest.main()
