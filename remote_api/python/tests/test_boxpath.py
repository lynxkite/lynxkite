import unittest
from lynx.kite import LynxKite, SideEffectCollector, subworkspace


lk = LynxKite()


class TestBoxPath(unittest.TestCase):

  def test_boxpath(self):

    @subworkspace
    def export_csv(t, path, sec=SideEffectCollector.AUTO):
      t.exportToCSV(path=path).register(sec)

    @subworkspace
    def export_eg(sec=SideEffectCollector.AUTO):
      eg = lk.createExampleGraph()
      export_csv(eg.sql('select name from vertices'), 'names').register(sec)
      export_csv(eg.sql('select age from vertices'), 'ages').register(sec)

    @subworkspace
    def main(sec=SideEffectCollector.AUTO):
      export_eg().register(sec)

    paths = set(str(p) for p in main().workspace.side_effect_paths())
    self.assertEqual(paths, set([
        'export_eg_?/export_csv_0/exportToCSV_0',
        'export_eg_?/export_csv_1/exportToCSV_0']))

  def test_find_all(self):
    def create_nested_workspace():
      @subworkspace
      def people():
        eg = lk.createExampleGraph()
        return eg.sql('select * from vertices', summary='People')

      @subworkspace
      def men(p):
        return p.sql('select * from input where gender = "Male"', summary='Men')

      @lk.workspace()
      def richest_man():
        p = people()
        m = men(p)
        return m.sql('select * from input order by income desc limit 1', summary='Richest')

      return richest_man

    nested_ws = create_nested_workspace()
    sql1s = [box_path.base for box_path in nested_ws.find_all('sql1')]
    summaries = sorted([box.parameters['summary'] for box in sql1s])
    self.assertListEqual(summaries, ['Men', 'People', 'Richest'])

  def test_snatch(self):

    def create_nested_workspace():
      @subworkspace
      def identity(x):
        return x

      @subworkspace
      def inside(x):
        return identity(x)

      @lk.workspace()
      def outside():
        return inside(lk.createExampleGraph())

      return outside

    nested_ws = create_nested_workspace()
    t = nested_ws.find('identity').snatch()
    data = t.sql('select * from vertices').get_table_data().data
    self.assertEqual(len(data), 4)
