import unittest
import lynx.kite
from lynx.kite import pp, text


class TestWorkspaceDecorator(unittest.TestCase):

  def test_ws_decorator_without_arguments(self):
    lk = lynx.kite.LynxKite()

    @lk.workspace()
    def join_ws(x, y):
      return dict(xjoin=lk.sql('select * from one cross join two', x, y))

    eg = lk.createExampleGraph()
    names = eg.sql('select name from vertices')
    ages = eg.sql('select age from vertices')
    table = join_ws(names, ages).get_table(lk)
    values = [(row[0].string, row[1].string) for row in table.data]
    self.assertEqual(len(values), 16)
    expected_result = [
        ('Adam', '20.3'), ('Adam', '18.2'), ('Adam', '50.3'), ('Adam', '2'),
        ('Eve', '20.3'), ('Eve', '18.2'), ('Eve', '50.3'), ('Eve', '2'),
        ('Bob', '20.3'), ('Bob', '18.2'), ('Bob', '50.3'), ('Bob', '2'),
        ('Isolated Joe', '20.3'), ('Isolated Joe', '18.2'),
        ('Isolated Joe', '50.3'), ('Isolated Joe', '2')]
    self.assertEqual(values, expected_result)

  def test_ws_decorator_with_ws_parameters(self):
    lk = lynx.kite.LynxKite()

    @lk.workspace(parameters=[text('a'), text('b'), text('c')])
    def add_ws():
      o = (lk.createVertices(size='5')
             .deriveScalar(output='total', expr=pp('${a.toInt+b.toInt+c.toInt}')))
      return dict(sc=o)

    project = add_ws(a='2', b='3', c='4').get_project(lk)
    scalars = {s.title: lk.get_scalar(s.id) for s in project.scalars}
    self.assertEqual(scalars['total'].string, '9')

  def test_multiple_ws_decorators(self):
    lk = lynx.kite.LynxKite()

    @lk.workspace(parameters=[text('field'), text('limit')])
    def filter_table(table):
      query = pp('select name, income from input where ${field} > ${limit}')
      out = table.sql(query)
      return dict(table=out)

    @lk.workspace()
    def graph_to_table(graph):
      return dict(table=graph.sql('select * from vertices'))

    @lk.workspace()
    def full_workflow():
      return dict(
          result=filter_table(
              graph_to_table(lk.createExampleGraph()),
              field='income',
              limit=500))

    table = full_workflow().get_table(lk)
    values = [(row[0].string, row[1].string) for row in table.data]
    self.assertEqual(values, [('Adam', '1000'), ('Bob', '2000')])
