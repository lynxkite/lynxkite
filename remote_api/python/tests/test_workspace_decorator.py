import unittest
import lynx.kite
from lynx.kite import workspace, pp, text


class TestWorkspaceDecorator(unittest.TestCase):

  def test_ws_decorator_without_arguments(self):
    lk = lynx.kite.LynxKite()

    @workspace()
    def join_ws(x, y):
      return dict(xjoin=lk.sql('select * from one cross join two', x, y))

    eg = lk.createExampleGraph()
    names = eg.sql('select name from vertices')
    ages = eg.sql('select age from vertices')
    state = lk.get_state_id(join_ws(names, ages))
    values = [(row[0].string, row[1].string) for row in lk.get_table(state).data]
    self.assertEqual(len(values), 16)
    expected_result = [
        ('Adam', '20.3'), ('Adam', '18.2'), ('Adam', '50.3'), ('Adam', '2'),
        ('Eve', '20.3'), ('Eve', '18.2'), ('Eve', '50.3'), ('Eve', '2'),
        ('Bob', '20.3'), ('Bob', '18.2'), ('Bob', '50.3'), ('Bob', '2'),
        ('Isolated Joe', '20.3'), ('Isolated Joe', '18.2'),
        ('Isolated Joe', '50.3'), ('Isolated Joe', '2')]
    self.assertEqual(values, expected_result)
