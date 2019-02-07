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
