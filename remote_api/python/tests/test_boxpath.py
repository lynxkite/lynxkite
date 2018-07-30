import unittest
from lynx.kite import LynxKite, SideEffectCollector, subworkspace


class TestBoxPath(unittest.TestCase):

  def test_boxpath(self):
    lk = LynxKite()

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
